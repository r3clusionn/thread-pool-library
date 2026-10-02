use std::any::Any;
use std::marker::PhantomData;
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};

use crate::handle::Latch;
use crate::pool::{Shared, Task};

struct State {
    running: AtomicUsize,
    panic: Mutex<Option<Box<dyn Any + Send + 'static>>>,
    latch: Latch,
}

/// Spawns tasks that may borrow from the stack of the code that called [`Handle::scope`](crate::Handle::scope).
pub struct Scope<'scope, 'env: 'scope> {
    shared: Arc<Shared>,
    state: Arc<State>,
    scope: PhantomData<&'scope mut &'scope ()>,
    env: PhantomData<&'env mut &'env ()>,
}

impl<'scope, 'env> Scope<'scope, 'env> {
    pub(crate) fn run<F, R>(shared: &Arc<Shared>, f: F) -> R
    where
        F: for<'s> FnOnce(&'s Scope<'s, 'env>) -> R,
    {
        let scope = Scope {
            shared: shared.clone(),
            state: Arc::new(State { running: AtomicUsize::new(0), panic: Mutex::new(None), latch: Latch::default() }),
            scope: PhantomData,
            env: PhantomData,
        };
        let result = catch_unwind(AssertUnwindSafe(|| f(&scope)));
        // The tasks borrow from the caller's stack, so nothing may return before they are done,
        // not even when `f` itself panicked.
        shared.help_until(|| scope.state.running.load(SeqCst) == 0, &scope.state.latch);
        let task_panic = scope.state.panic.lock().unwrap_or_else(|e| e.into_inner()).take();
        match result {
            Err(p) => resume_unwind(p),
            Ok(r) => match task_panic {
                Some(p) => resume_unwind(p),
                None => r,
            },
        }
    }

    /// Queues a task. It may borrow anything that outlives the scope. The queue capacity of the
    /// pool does not apply. If the pool has already been shut down the task runs on the calling
    /// thread.
    pub fn spawn<F>(&'scope self, f: F)
    where
        F: FnOnce() + Send + 'scope,
    {
        self.state.running.fetch_add(1, SeqCst);
        let state = self.state.clone();
        let task = move || {
            if let Err(p) = catch_unwind(AssertUnwindSafe(f)) {
                let mut slot = state.panic.lock().unwrap_or_else(|e| e.into_inner());
                if slot.is_none() {
                    *slot = Some(p);
                }
            }
            if state.running.fetch_sub(1, SeqCst) == 1 {
                state.latch.notify_all();
            }
        };
        let boxed: Box<dyn FnOnce() + Send + 'scope> = Box::new(task);
        // SAFETY: `run` does not return until `running` is zero, i.e. until every task has finished
        // executing, so nothing the task borrows is used after the scope's lifetime ends.
        let task: Task = unsafe { std::mem::transmute(boxed) };
        if let Err(task) = self.shared.submit_scoped(task) {
            task();
        }
    }
}
