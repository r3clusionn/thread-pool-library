//! A tour of the API with real output: results, a scope that borrows, a panic, a bounded queue and
//! shutdown.

use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::thread;
use std::time::Duration;

use workpool::{SubmitError, ThreadPool};

fn main() {
    // The demo panics on purpose; keep the default hook's stderr message out of the output.
    std::panic::set_hook(Box::new(|_| {}));
    let pool = ThreadPool::builder().threads(4).name("demo").build().unwrap();
    println!("pool: {} workers, work stealing, unbounded queue\n", pool.threads());

    let handles: Vec<_> = (1..=5u64).map(|n| pool.spawn(move || (n, (1..=n).product::<u64>()))).collect();
    for h in handles {
        let (n, f) = h.join().unwrap();
        println!("spawn: {n}! = {f}");
    }

    let words = ["alpha", "beta", "gamma", "delta"];
    let mut lengths = vec![0usize; words.len()];
    pool.scope(|s| {
        for (w, slot) in words.iter().zip(lengths.iter_mut()) {
            s.spawn(move || *slot = w.len());
        }
    });
    println!("scope: tasks borrowed the stack, lengths = {lengths:?}");

    let bad = pool.spawn(|| -> u32 { panic!("division by zero in task") });
    match bad.join() {
        Err(e) => println!("panic: caught, the pool carries on ({e})"),
        Ok(_) => unreachable!(),
    }
    println!("after the panic: spawn still works, 6 * 7 = {}", pool.spawn(|| 6 * 7).join().unwrap());
    pool.join();

    let small = ThreadPool::builder().threads(1).queue_capacity(2).build().unwrap();
    small.execute(|| thread::sleep(Duration::from_millis(100))).unwrap();
    thread::sleep(Duration::from_millis(20)); // the worker is now busy
    let outcomes: Vec<_> = (0..4).map(|_| small.try_execute(|| {})).collect();
    let accepted = outcomes.iter().filter(|r| r.is_ok()).count();
    let full = outcomes.iter().filter(|r| **r == Err(SubmitError::Full)).count();
    println!("bounded queue (capacity 2, worker busy): {accepted} accepted, {full} refused as Full");

    let done = std::sync::Arc::new(AtomicUsize::new(0));
    for _ in 0..100 {
        let d = done.clone();
        pool.execute(move || {
            thread::sleep(Duration::from_micros(200));
            d.fetch_add(1, SeqCst);
        })
        .unwrap();
    }
    let handle = pool.handle();
    drop(pool);
    println!("graceful shutdown: {} of 100 queued tasks ran before drop returned", done.load(SeqCst));
    println!("submit after shutdown: {:?}", handle.execute(|| {}));
}
