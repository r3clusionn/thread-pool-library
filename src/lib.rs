//! A thread pool.
//!
//! ```
//! use workpool::ThreadPool;
//!
//! let pool = ThreadPool::new(4);
//! let answer = pool.spawn(|| 6 * 7);
//! assert_eq!(answer.join().unwrap(), 42);
//!
//! // Tasks can borrow from the caller's stack inside a scope.
//! let mut squares = vec![0u64; 8];
//! pool.scope(|s| {
//!     for (i, slot) in squares.iter_mut().enumerate() {
//!         s.spawn(move || *slot = (i * i) as u64);
//!     }
//! });
//! assert_eq!(squares[7], 49);
//! ```
//!
//! Two queue layouts are available, chosen with [`Builder::strategy`]:
//!
//! * [`Strategy::Shared`]: one FIFO queue that every worker pulls from.
//! * [`Strategy::WorkStealing`]: a deque per worker plus a shared injector. A task spawned from a
//!   worker goes to that worker's own deque and runs there last-in first-out; idle workers steal
//!   half of a busy worker's deque from the front.
//!
//! Panics inside tasks are caught: the worker survives and the panic is reported through
//! [`JoinHandle::join`] or counted in [`Stats::panicked`].

mod handle;
mod pool;
mod scope;

pub use handle::{JoinError, JoinHandle};
pub use pool::{Builder, Handle, Stats, Strategy, SubmitError, ThreadPool};
pub use scope::Scope;
