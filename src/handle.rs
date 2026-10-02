use std::any::Any;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::pool::Shared;

/// A mutex and condition variable that a waiting thread can sleep on.
#[derive(Default)]
pub(crate) struct Latch {
    mu: Mutex<()>,
    cv: Condvar,
}

impl Latch {
    pub(crate) fn notify_all(&self) {
        let _g = self.mu.lock().unwrap_or_else(|e| e.into_inner());
        self.cv.notify_all();
    }

    /// Sleeps for at most `timeout` unless `done` already holds. The check happens under the lock
    /// that `notify_all` takes, so a notification between the caller's check and the wait is not lost.
    pub(crate) fn wait(&self, done: impl Fn() -> bool, timeout: Duration) {
        let g = self.mu.lock().unwrap_or_else(|e| e.into_inner());
        if !done() {
            let _ = self.cv.wait_timeout(g, timeout);
        }
    }
}

/// Why [`JoinHandle::join`] did not produce a value.
pub enum JoinError {
    /// The task panicked. The payload is what it panicked with.
    Panicked(Box<dyn Any + Send + 'static>),
    /// The task was discarded before it ran, by [`ThreadPool::shutdown_now`](crate::ThreadPool::shutdown_now).
    Cancelled,
}

impl JoinError {
    /// The panic message, if the task panicked with a string.
    pub fn message(&self) -> Option<&str> {
        match self {
            JoinError::Panicked(p) => p
                .downcast_ref::<&'static str>()
                .copied()
                .or_else(|| p.downcast_ref::<String>().map(String::as_str)),
            JoinError::Cancelled => None,
        }
    }
}

impl fmt::Debug for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinError::Panicked(_) => write!(f, "Panicked({:?})", self.message()),
            JoinError::Cancelled => write!(f, "Cancelled"),
        }
    }
}

impl fmt::Display for JoinError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinError::Panicked(_) => match self.message() {
                Some(m) => write!(f, "task panicked: {m}"),
                None => write!(f, "task panicked"),
            },
            JoinError::Cancelled => write!(f, "task was cancelled before it ran"),
        }
    }
}

impl std::error::Error for JoinError {}

pub(crate) struct Slot<T> {
    value: Mutex<Option<Result<T, JoinError>>>,
    done: AtomicBool,
    latch: Latch,
}

impl<T> Slot<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Slot { value: Mutex::new(None), done: AtomicBool::new(false), latch: Latch::default() })
    }

    pub(crate) fn fill(&self, r: Result<T, JoinError>) {
        let mut v = self.value.lock().unwrap_or_else(|e| e.into_inner());
        if v.is_none() {
            *v = Some(r);
            drop(v);
            self.done.store(true, Ordering::SeqCst);
            self.latch.notify_all();
        }
    }
}

/// Fills the slot with `Cancelled` if the task is dropped without having run.
pub(crate) struct Completer<T>(pub(crate) Arc<Slot<T>>);

impl<T> Drop for Completer<T> {
    fn drop(&mut self) {
        self.0.fill(Err(JoinError::Cancelled));
    }
}

/// The result of a task started with [`Handle::spawn`](crate::Handle::spawn).
pub struct JoinHandle<T> {
    pub(crate) slot: Arc<Slot<T>>,
    pub(crate) shared: Arc<Shared>,
}

impl<T> JoinHandle<T> {
    /// True once the task has finished, panicked or been cancelled.
    pub fn is_finished(&self) -> bool {
        self.slot.done.load(Ordering::SeqCst)
    }

    /// Waits for the task. A pool worker that joins runs other queued tasks while it waits, so
    /// joining from inside a task cannot deadlock the pool; any other thread simply sleeps.
    pub fn join(self) -> Result<T, JoinError> {
        let slot = &self.slot;
        self.shared.help_until(|| slot.done.load(Ordering::SeqCst), &slot.latch);
        slot.value.lock().unwrap_or_else(|e| e.into_inner()).take().expect("slot filled before done is set")
    }
}

impl<T> fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JoinHandle {{ finished: {} }}", self.is_finished())
    }
}
