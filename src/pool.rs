use std::cell::Cell;
use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

use crate::handle::{Completer, JoinError, JoinHandle, Latch, Slot};
use crate::scope::Scope;

pub(crate) type Task = Box<dyn FnOnce() + Send + 'static>;

/// How queued tasks are laid out and distributed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Strategy {
    /// One FIFO queue shared by every worker.
    Shared,
    /// A deque per worker plus a shared injector queue. Idle workers steal half of a victim's deque.
    WorkStealing,
}

/// Why a task was not accepted. The task is dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubmitError {
    /// The pool is shutting down.
    Closed,
    /// The queue is at its capacity ([`Handle::try_execute`] only).
    Full,
}

impl fmt::Display for SubmitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SubmitError::Closed => write!(f, "the pool is shut down"),
            SubmitError::Full => write!(f, "the task queue is full"),
        }
    }
}

impl std::error::Error for SubmitError {}

/// Counters for a pool. Reading them does not stop the workers, so the numbers are a moment's view.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub threads: usize,
    /// Tasks that have finished, including ones that panicked.
    pub executed: u64,
    /// Tasks moved from one worker's deque to another's by stealing.
    pub stolen: u64,
    /// Tasks that panicked.
    pub panicked: u64,
    /// Tasks waiting in a queue.
    pub queued: usize,
    /// Tasks waiting or running.
    pub pending: usize,
}

thread_local! {
    /// (pool id, worker index) when the current thread is a pool worker.
    static WORKER: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
}

static NEXT_ID: AtomicUsize = AtomicUsize::new(1);

/// Failed searches a worker makes before it sleeps.
const SPIN_ROUNDS: u32 = 64;

pub(crate) struct Shared {
    id: usize,
    strategy: Strategy,
    threads: usize,
    capacity: Option<usize>,
    injector: Mutex<VecDeque<Task>>,
    locals: Vec<Mutex<VecDeque<Task>>>,
    /// Tasks sitting in a queue. Incremented before the push, decremented when a worker takes one.
    queued: AtomicUsize,
    /// Tasks submitted and not yet finished (queued or running).
    pending: AtomicUsize,
    sleepers: AtomicUsize,
    sleep: Mutex<()>,
    wake: Condvar,
    idle: Latch,
    space_waiters: AtomicUsize,
    space: Mutex<()>,
    space_cv: Condvar,
    /// No new tasks are accepted. Workers exit once nothing is pending.
    closing: AtomicBool,
    /// Workers exit as soon as they finish their current task.
    abort: AtomicBool,
    executed: AtomicU64,
    stolen: AtomicU64,
    panicked: Arc<AtomicU64>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    // A task panic never happens while a queue lock is held, but do not turn a poisoned lock into
    // a second panic if one ever does.
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Shared {
    fn on_worker(&self) -> Option<usize> {
        WORKER.with(|w| w.get()).and_then(|(id, i)| (id == self.id).then_some(i))
    }

    fn submit(&self, task: Task, block: bool, ignore_capacity: bool) -> Result<(), (SubmitError, Task)> {
        // `pending` goes up before the closing check so that a worker that sees closing and
        // pending == 0 can be sure no submission is in flight (see `worker_loop`).
        self.pending.fetch_add(1, SeqCst);
        if self.closing.load(SeqCst) {
            self.finish_one();
            return Err((SubmitError::Closed, task));
        }
        let from_worker = self.on_worker();
        match self.capacity {
            // A worker waiting for room could be waiting for itself, so workers never block.
            Some(cap) if !ignore_capacity && from_worker.is_none() => loop {
                let q = self.queued.load(SeqCst);
                if q < cap {
                    if self.queued.compare_exchange(q, q + 1, SeqCst, SeqCst).is_ok() {
                        break;
                    }
                    continue;
                }
                if !block {
                    self.finish_one();
                    return Err((SubmitError::Full, task));
                }
                let mut g = lock(&self.space);
                self.space_waiters.fetch_add(1, SeqCst);
                while self.queued.load(SeqCst) >= cap && !self.closing.load(SeqCst) {
                    g = self.space_cv.wait(g).unwrap_or_else(|e| e.into_inner());
                }
                self.space_waiters.fetch_sub(1, SeqCst);
                drop(g);
                if self.closing.load(SeqCst) {
                    self.finish_one();
                    return Err((SubmitError::Closed, task));
                }
            },
            _ => {
                self.queued.fetch_add(1, SeqCst);
            }
        }
        match (self.strategy, from_worker) {
            (Strategy::WorkStealing, Some(me)) => lock(&self.locals[me]).push_back(task),
            _ => lock(&self.injector).push_back(task),
        }
        if self.sleepers.load(SeqCst) > 0 {
            let _g = lock(&self.sleep);
            self.wake.notify_one();
        }
        Ok(())
    }

    /// Marks one submitted task as finished, waking `join` callers and, while closing, idle workers.
    fn finish_one(&self) {
        if self.pending.fetch_sub(1, SeqCst) == 1 {
            self.idle.notify_all();
            if self.closing.load(SeqCst) {
                let _g = lock(&self.sleep);
                self.wake.notify_all();
            }
        }
    }

    /// Finds a task for worker `me`.
    fn find(&self, me: usize, rng: &mut u64) -> Option<Task> {
        match self.strategy {
            Strategy::Shared => lock(&self.injector).pop_front(),
            Strategy::WorkStealing => {
                if let Some(t) = lock(&self.locals[me]).pop_back() {
                    return Some(t);
                }
                if let Some(t) = lock(&self.injector).pop_front() {
                    return Some(t);
                }
                let n = self.locals.len();
                *rng ^= *rng << 13;
                *rng ^= *rng >> 7;
                *rng ^= *rng << 17;
                let start = (*rng % n as u64) as usize;
                for i in 0..n {
                    let victim = (start + i) % n;
                    if victim == me {
                        continue;
                    }
                    let mut batch: Vec<Task> = {
                        let mut q = lock(&self.locals[victim]);
                        let take = q.len().div_ceil(2);
                        q.drain(..take).collect()
                    };
                    if batch.is_empty() {
                        continue;
                    }
                    self.stolen.fetch_add(batch.len() as u64, SeqCst);
                    let first = batch.remove(0);
                    if !batch.is_empty() {
                        lock(&self.locals[me]).extend(batch);
                    }
                    return Some(first);
                }
                None
            }
        }
    }

    fn run(&self, task: Task) {
        self.queued.fetch_sub(1, SeqCst);
        if self.capacity.is_some() && self.space_waiters.load(SeqCst) > 0 {
            let _g = lock(&self.space);
            self.space_cv.notify_one();
        }
        if catch_unwind(AssertUnwindSafe(task)).is_err() {
            self.panicked.fetch_add(1, SeqCst);
        }
        self.executed.fetch_add(1, SeqCst);
        self.finish_one();
    }

    /// Waits until `done` holds. A pool worker runs other queued tasks meanwhile, so that waiting
    /// inside a task cannot starve the pool of workers; any other thread sleeps on `latch`.
    pub(crate) fn help_until(&self, done: impl Fn() -> bool, latch: &Latch) {
        let me = self.on_worker();
        let mut rng = 0x9E37_79B9_7F4A_7C15 ^ (self as *const _ as u64);
        while !done() {
            match me.and_then(|me| self.find(me, &mut rng)) {
                Some(t) => self.run(t),
                None if me.is_some() => latch.wait(&done, Duration::from_micros(200)),
                None => latch.wait(&done, Duration::from_millis(50)),
            }
        }
    }

    /// Submission used by `scope`: never refused for capacity.
    pub(crate) fn submit_scoped(&self, task: Task) -> Result<(), Task> {
        self.submit(task, false, true).map_err(|(_, task)| task)
    }

    /// Drops every queued task and returns how many there were.
    fn discard_queued(&self) -> usize {
        let mut tasks: Vec<Task> = Vec::new();
        tasks.extend(lock(&self.injector).drain(..));
        for l in &self.locals {
            tasks.extend(lock(l).drain(..));
        }
        let n = tasks.len();
        self.queued.fetch_sub(n, SeqCst);
        drop(tasks);
        for _ in 0..n {
            self.finish_one();
        }
        n
    }

    fn worker_loop(&self, me: usize) {
        WORKER.with(|w| w.set(Some((self.id, me))));
        let mut rng = 0x2545_F491_4F6C_DD1D_u64.wrapping_mul(me as u64 + 1) | 1;
        let mut idle_rounds = 0;
        loop {
            if self.abort.load(SeqCst) {
                break;
            }
            if let Some(t) = self.find(me, &mut rng) {
                idle_rounds = 0;
                self.run(t);
                continue;
            }
            // Spin briefly before sleeping: a producer that is still pushing then does not pay for a
            // wake-up per task.
            if idle_rounds < SPIN_ROUNDS {
                idle_rounds += 1;
                if idle_rounds % 16 == 0 {
                    thread::yield_now();
                } else {
                    std::hint::spin_loop();
                }
                continue;
            }
            idle_rounds = 0;
            if self.queued.load(SeqCst) > 0 {
                // A task is on its way into a queue or between two queues; look again.
                thread::yield_now();
                continue;
            }
            let g = lock(&self.sleep);
            self.sleepers.fetch_add(1, SeqCst);
            // A submitter increments `queued` before it reads `sleepers`, and this thread
            // increments `sleepers` before it reads `queued`, so one of the two sees the other.
            // The notification is sent under `sleep`, so it cannot fall between this check and the wait.
            let exit = self.abort.load(SeqCst) || (self.closing.load(SeqCst) && self.pending.load(SeqCst) == 0);
            if exit {
                self.sleepers.fetch_sub(1, SeqCst);
                break;
            }
            if self.queued.load(SeqCst) == 0 {
                drop(self.wake.wait(g).unwrap_or_else(|e| e.into_inner()));
            }
            self.sleepers.fetch_sub(1, SeqCst);
        }
        WORKER.with(|w| w.set(None));
    }
}

/// Builds a [`ThreadPool`].
#[derive(Clone, Debug)]
pub struct Builder {
    threads: usize,
    strategy: Strategy,
    capacity: Option<usize>,
    name: String,
    stack_size: Option<usize>,
}

impl Default for Builder {
    fn default() -> Self {
        Builder {
            threads: thread::available_parallelism().map_or(1, |n| n.get()),
            strategy: Strategy::WorkStealing,
            capacity: None,
            name: "workpool".to_string(),
            stack_size: None,
        }
    }
}

impl Builder {
    /// Number of worker threads (at least 1). Defaults to the number of logical CPUs.
    pub fn threads(mut self, n: usize) -> Self {
        self.threads = n.max(1);
        self
    }

    /// Queue layout. Defaults to [`Strategy::WorkStealing`].
    pub fn strategy(mut self, s: Strategy) -> Self {
        self.strategy = s;
        self
    }

    /// Bounds the number of queued (not yet started) tasks. A thread outside the pool that calls
    /// [`Handle::execute`] when the queue is full waits for room; [`Handle::try_execute`] returns
    /// [`SubmitError::Full`]. Tasks submitted from inside the pool and from a scope are never
    /// refused, because a worker that waits for room could be waiting for itself.
    pub fn queue_capacity(mut self, n: usize) -> Self {
        self.capacity = Some(n.max(1));
        self
    }

    /// Prefix for worker thread names (`name-0`, `name-1`, ...).
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    pub fn stack_size(mut self, bytes: usize) -> Self {
        self.stack_size = Some(bytes);
        self
    }

    pub fn build(self) -> io::Result<ThreadPool> {
        let shared = Arc::new(Shared {
            id: NEXT_ID.fetch_add(1, SeqCst),
            strategy: self.strategy,
            threads: self.threads,
            capacity: self.capacity,
            injector: Mutex::new(VecDeque::new()),
            locals: (0..self.threads).map(|_| Mutex::new(VecDeque::new())).collect(),
            queued: AtomicUsize::new(0),
            pending: AtomicUsize::new(0),
            sleepers: AtomicUsize::new(0),
            sleep: Mutex::new(()),
            wake: Condvar::new(),
            idle: Latch::default(),
            space_waiters: AtomicUsize::new(0),
            space: Mutex::new(()),
            space_cv: Condvar::new(),
            closing: AtomicBool::new(false),
            abort: AtomicBool::new(false),
            executed: AtomicU64::new(0),
            stolen: AtomicU64::new(0),
            panicked: Arc::new(AtomicU64::new(0)),
        });
        let mut workers = Vec::with_capacity(self.threads);
        for i in 0..self.threads {
            let s = shared.clone();
            let mut b = thread::Builder::new().name(format!("{}-{i}", self.name));
            if let Some(sz) = self.stack_size {
                b = b.stack_size(sz);
            }
            match b.spawn(move || s.worker_loop(i)) {
                Ok(h) => workers.push(h),
                Err(e) => {
                    shared.closing.store(true, SeqCst);
                    shared.abort.store(true, SeqCst);
                    {
                        let _g = lock(&shared.sleep);
                        shared.wake.notify_all();
                    }
                    for h in workers {
                        let _ = h.join();
                    }
                    return Err(e);
                }
            }
        }
        Ok(ThreadPool { handle: Handle { shared }, workers })
    }
}

/// A cloneable way to submit tasks to a pool, for use from inside tasks and other threads. A handle
/// does not keep the workers alive: once the [`ThreadPool`] is shut down, submissions fail with
/// [`SubmitError::Closed`].
#[derive(Clone)]
pub struct Handle {
    pub(crate) shared: Arc<Shared>,
}

impl Handle {
    /// Queues a task. Waits for room if the pool has a bounded queue that is full and the caller
    /// is not a worker.
    pub fn execute<F: FnOnce() + Send + 'static>(&self, f: F) -> Result<(), SubmitError> {
        self.shared.submit(Box::new(f), true, false).map_err(|(e, _)| e)
    }

    /// Like [`execute`](Self::execute) but returns [`SubmitError::Full`] instead of waiting.
    pub fn try_execute<F: FnOnce() + Send + 'static>(&self, f: F) -> Result<(), SubmitError> {
        self.shared.submit(Box::new(f), false, false).map_err(|(e, _)| e)
    }

    /// Queues a task and returns a handle for its result. If the pool is closed the handle's
    /// `join` reports [`JoinError::Cancelled`].
    pub fn spawn<T, F>(&self, f: F) -> JoinHandle<T>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let slot = Slot::new();
        let completer = Completer(slot.clone());
        let panicked = self.shared.panicked.clone();
        let task = move || {
            let c = completer;
            match catch_unwind(AssertUnwindSafe(f)) {
                Ok(v) => c.0.fill(Ok(v)),
                Err(p) => {
                    panicked.fetch_add(1, SeqCst);
                    c.0.fill(Err(JoinError::Panicked(p)))
                }
            }
        };
        // On refusal the closure is dropped, which fills the slot with `Cancelled`.
        let _ = self.shared.submit(Box::new(task), true, false);
        JoinHandle { slot, shared: self.shared.clone() }
    }

    /// Runs `f` with a [`Scope`] whose tasks may borrow from the caller's stack, and returns when
    /// `f` and every task spawned in the scope have finished. A pool worker that calls this runs
    /// queued tasks while it waits. If any task panicked, the first panic is resumed here after all tasks ended.
    pub fn scope<'env, F, R>(&self, f: F) -> R
    where
        F: for<'scope> FnOnce(&'scope Scope<'scope, 'env>) -> R,
    {
        Scope::run(&self.shared, f)
    }

    /// Number of worker threads.
    pub fn threads(&self) -> usize {
        self.shared.threads
    }

    pub fn stats(&self) -> Stats {
        let s = &self.shared;
        Stats {
            threads: s.threads,
            executed: s.executed.load(SeqCst),
            stolen: s.stolen.load(SeqCst),
            panicked: s.panicked.load(SeqCst),
            queued: s.queued.load(SeqCst),
            pending: s.pending.load(SeqCst),
        }
    }
}

impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Handle({:?})", self.stats())
    }
}

/// A fixed set of worker threads. Dropping the pool is a graceful shutdown: no new tasks are
/// accepted, every queued task runs, and the workers are joined.
pub struct ThreadPool {
    handle: Handle,
    workers: Vec<thread::JoinHandle<()>>,
}

impl ThreadPool {
    /// A pool of `threads` workers (at least 1) with work stealing and an unbounded queue.
    pub fn new(threads: usize) -> ThreadPool {
        Builder::default().threads(threads).build().expect("failed to spawn worker threads")
    }

    pub fn builder() -> Builder {
        Builder::default()
    }

    /// A cloneable submit-only handle, to give to tasks or other threads.
    pub fn handle(&self) -> Handle {
        self.handle.clone()
    }

    /// Waits until every submitted task has finished. Panics if called from one of the pool's own
    /// workers, which would wait for itself.
    pub fn join(&self) {
        let s = &self.handle.shared;
        assert!(s.on_worker().is_none(), "ThreadPool::join called from a worker of the same pool");
        while s.pending.load(SeqCst) > 0 {
            s.idle.wait(|| s.pending.load(SeqCst) == 0, Duration::from_millis(50));
        }
    }

    /// Graceful shutdown: stops accepting tasks, runs everything already queued, joins the workers.
    pub fn shutdown(mut self) {
        self.stop(false);
    }

    /// Stops accepting tasks, discards the queued ones (their [`JoinHandle`]s report
    /// [`JoinError::Cancelled`]), lets running tasks finish and joins the workers. Returns how
    /// many tasks were discarded.
    pub fn shutdown_now(mut self) -> usize {
        self.stop(true)
    }

    fn stop(&mut self, discard: bool) -> usize {
        let s = &self.handle.shared;
        s.closing.store(true, SeqCst);
        let mut dropped = 0;
        if discard {
            s.abort.store(true, SeqCst);
            dropped += s.discard_queued();
        }
        {
            let _g = lock(&s.sleep);
            s.wake.notify_all();
        }
        {
            let _g = lock(&s.space);
            s.space_cv.notify_all();
        }
        for w in self.workers.drain(..) {
            let _ = w.join();
        }
        if discard {
            // A task that was between two queues during the first pass has landed by now.
            dropped += s.discard_queued();
        }
        dropped
    }
}

impl std::ops::Deref for ThreadPool {
    type Target = Handle;
    fn deref(&self) -> &Handle {
        &self.handle
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        if !self.workers.is_empty() {
            self.stop(false);
        }
    }
}

impl fmt::Debug for ThreadPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ThreadPool({:?})", self.handle.stats())
    }
}
