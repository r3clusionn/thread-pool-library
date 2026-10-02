use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use workpool::{JoinError, Strategy, SubmitError, ThreadPool};

const STRATEGIES: [Strategy; 2] = [Strategy::Shared, Strategy::WorkStealing];

fn pool(threads: usize, s: Strategy) -> ThreadPool {
    ThreadPool::builder().threads(threads).strategy(s).build().unwrap()
}

#[test]
fn runs_every_task_exactly_once() {
    for s in STRATEGIES {
        let p = pool(4, s);
        let counts: Arc<Vec<AtomicUsize>> = Arc::new((0..10_000).map(|_| AtomicUsize::new(0)).collect());
        for i in 0..10_000 {
            let c = counts.clone();
            p.execute(move || {
                c[i].fetch_add(1, SeqCst);
            })
            .unwrap();
        }
        p.join();
        assert!(counts.iter().all(|c| c.load(SeqCst) == 1), "{s:?}");
        assert_eq!(p.stats().executed, 10_000);
        assert_eq!(p.stats().pending, 0);
    }
}

#[test]
fn spawn_returns_values_in_any_completion_order() {
    for s in STRATEGIES {
        let p = pool(3, s);
        let hs: Vec<_> = (0..200u64).map(|i| p.spawn(move || i * i)).collect();
        let sum: u64 = hs.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(sum, (0..200u64).map(|i| i * i).sum::<u64>());
    }
}

#[test]
fn a_panicking_task_does_not_kill_the_worker() {
    for s in STRATEGIES {
        let p = pool(1, s);
        let bad = p.spawn(|| -> u32 { panic!("boom") });
        let err = bad.join().unwrap_err();
        assert!(matches!(err, JoinError::Panicked(_)));
        assert_eq!(err.message(), Some("boom"));
        p.execute(|| panic!("also boom")).unwrap();
        // The single worker is still alive and runs later tasks.
        assert_eq!(p.spawn(|| 7).join().unwrap(), 7);
        p.join();
        assert_eq!(p.stats().panicked, 2);
    }
}

#[test]
fn graceful_shutdown_runs_everything_queued() {
    for s in STRATEGIES {
        let done = Arc::new(AtomicUsize::new(0));
        let p = pool(2, s);
        for _ in 0..500 {
            let d = done.clone();
            p.execute(move || {
                thread::sleep(Duration::from_micros(50));
                d.fetch_add(1, SeqCst);
            })
            .unwrap();
        }
        drop(p);
        assert_eq!(done.load(SeqCst), 500, "{s:?}");
    }
}

#[test]
fn shutdown_now_discards_queued_tasks_and_cancels_their_handles() {
    for s in STRATEGIES {
        let p = pool(1, s);
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let blocker = p.spawn(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            1
        });
        started_rx.recv().unwrap();
        let ran = Arc::new(AtomicUsize::new(0));
        let queued: Vec<_> = (0..50)
            .map(|_| {
                let r = ran.clone();
                p.spawn(move || {
                    r.fetch_add(1, SeqCst);
                })
            })
            .collect();
        let h = p.handle();
        let t = thread::spawn(move || p.shutdown_now());
        // Let shutdown_now start, then let the running task finish.
        thread::sleep(Duration::from_millis(50));
        release_tx.send(()).unwrap();
        let dropped = t.join().unwrap();
        assert_eq!(dropped, 50, "{s:?}");
        assert_eq!(blocker.join().unwrap(), 1);
        for q in queued {
            assert!(matches!(q.join(), Err(JoinError::Cancelled)));
        }
        assert_eq!(ran.load(SeqCst), 0);
        assert_eq!(h.execute(|| {}), Err(SubmitError::Closed));
    }
}

#[test]
fn submitting_after_shutdown_fails() {
    let p = pool(2, Strategy::WorkStealing);
    let h = p.handle();
    p.shutdown();
    assert_eq!(h.execute(|| {}), Err(SubmitError::Closed));
    assert!(matches!(h.spawn(|| 1).join(), Err(JoinError::Cancelled)));
}

#[test]
fn try_execute_reports_a_full_queue() {
    for s in STRATEGIES {
        let p = ThreadPool::builder().threads(1).strategy(s).queue_capacity(2).build().unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let gate = Arc::new(Barrier::new(2));
        let g = gate.clone();
        p.execute(move || {
            started_tx.send(()).unwrap();
            g.wait();
        })
        .unwrap();
        started_rx.recv().unwrap(); // the worker is busy, the queue is empty
        assert_eq!(p.try_execute(|| {}), Ok(()));
        assert_eq!(p.try_execute(|| {}), Ok(()));
        assert_eq!(p.try_execute(|| {}), Err(SubmitError::Full), "{s:?}");
        gate.wait();
        p.join();
        assert_eq!(p.try_execute(|| {}), Ok(()));
    }
}

#[test]
fn execute_blocks_until_there_is_room() {
    let p = ThreadPool::builder().threads(1).queue_capacity(1).build().unwrap();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    p.execute(move || {
        started_tx.send(()).unwrap();
        release_rx.recv().unwrap();
    })
    .unwrap();
    started_rx.recv().unwrap();
    p.execute(|| {}).unwrap(); // fills the queue
    let h = p.handle();
    let blocked = thread::spawn(move || {
        let t = Instant::now();
        h.execute(|| {}).unwrap();
        t.elapsed()
    });
    thread::sleep(Duration::from_millis(100));
    assert!(!blocked.is_finished(), "execute should be waiting for room");
    release_tx.send(()).unwrap();
    assert!(blocked.join().unwrap() >= Duration::from_millis(80));
    p.join();
}

#[test]
fn scope_tasks_borrow_the_stack() {
    for s in STRATEGIES {
        let p = pool(4, s);
        let mut data = vec![0u64; 1000];
        let total = AtomicUsize::new(0);
        p.scope(|sc| {
            for (i, chunk) in data.chunks_mut(100).enumerate() {
                let total = &total;
                sc.spawn(move || {
                    for x in chunk.iter_mut() {
                        *x = i as u64;
                    }
                    total.fetch_add(1, SeqCst);
                });
            }
        });
        assert_eq!(total.load(SeqCst), 10);
        assert_eq!(data[999], 9);
        assert_eq!(data[0], 0);
    }
}

#[test]
fn scope_resumes_a_task_panic_after_all_tasks_finished() {
    let p = pool(2, Strategy::WorkStealing);
    let finished = AtomicUsize::new(0);
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        p.scope(|sc| {
            sc.spawn(|| panic!("scoped boom"));
            for _ in 0..20 {
                sc.spawn(|| {
                    thread::sleep(Duration::from_millis(5));
                    finished.fetch_add(1, SeqCst);
                });
            }
        });
    }));
    assert!(r.is_err());
    assert_eq!(finished.load(SeqCst), 20);
    assert_eq!(p.spawn(|| 5).join().unwrap(), 5);
}

fn fib(h: &workpool::Handle, n: u64) -> u64 {
    if n < 2 {
        return n;
    }
    if n < 12 {
        return fib(h, n - 1) + fib(h, n - 2);
    }
    let (mut a, mut b) = (0, 0);
    h.scope(|sc| {
        sc.spawn(|| a = fib(h, n - 1));
        sc.spawn(|| b = fib(h, n - 2));
    });
    a + b
}

#[test]
fn nested_scopes_do_not_deadlock_even_on_one_thread() {
    for s in STRATEGIES {
        for threads in [1, 2, 8] {
            let p = pool(threads, s);
            assert_eq!(fib(&p.handle(), 24), 46368, "{s:?} x{threads}");
        }
    }
}

#[test]
fn joining_from_inside_a_task_does_not_deadlock() {
    let p = pool(1, Strategy::WorkStealing);
    let h = p.handle();
    let outer = p.spawn(move || {
        let inner = h.spawn(|| 21);
        inner.join().unwrap() * 2
    });
    assert_eq!(outer.join().unwrap(), 42);
}

fn spread(h: workpool::Handle, n: Arc<AtomicUsize>, depth: u32) {
    n.fetch_add(1, SeqCst);
    if depth > 0 {
        for _ in 0..3 {
            let (h2, n2) = (h.clone(), n.clone());
            h.execute(move || spread(h2, n2, depth - 1)).unwrap();
        }
    }
}

#[test]
fn tasks_can_spawn_tasks() {
    for s in STRATEGIES {
        let p = pool(4, s);
        let n = Arc::new(AtomicUsize::new(0));
        spread(p.handle(), n.clone(), 6);
        p.join();
        assert_eq!(n.load(SeqCst), (3usize.pow(7) - 1) / 2, "{s:?}");
    }
}

#[test]
fn work_stealing_moves_work_to_idle_workers() {
    let p = pool(4, Strategy::WorkStealing);
    let h = p.handle();
    let seen = Arc::new(std::sync::Mutex::new(std::collections::HashSet::new()));
    let seen2 = seen.clone();
    // One task on one worker spawns many tasks into that worker's own deque; the others must steal.
    p.spawn(move || {
        for _ in 0..400 {
            let s = seen2.clone();
            h.execute(move || {
                thread::sleep(Duration::from_micros(200));
                s.lock().unwrap().insert(thread::current().id());
            })
            .unwrap();
        }
    })
    .join()
    .unwrap();
    p.join();
    assert!(seen.lock().unwrap().len() >= 2, "only one worker ran the tasks");
    assert!(p.stats().stolen > 0);
}

#[test]
fn no_lost_wakeups_between_idle_gaps() {
    // Workers fall asleep between bursts; every burst must still complete promptly.
    for s in STRATEGIES {
        let p = pool(8, s);
        for round in 0..300 {
            let n = Arc::new(AtomicUsize::new(0));
            for _ in 0..(round % 7 + 1) {
                let n = n.clone();
                p.execute(move || {
                    n.fetch_add(1, SeqCst);
                })
                .unwrap();
            }
            let start = Instant::now();
            p.join();
            assert_eq!(n.load(SeqCst), round % 7 + 1);
            assert!(start.elapsed() < Duration::from_secs(5), "{s:?} round {round} stalled");
            if round % 10 == 0 {
                thread::sleep(Duration::from_millis(2));
            }
        }
    }
}

#[test]
fn many_producers_one_pool() {
    for s in STRATEGIES {
        let p = ThreadPool::builder().threads(4).strategy(s).queue_capacity(64).build().unwrap();
        let n = Arc::new(AtomicUsize::new(0));
        let producers: Vec<_> = (0..8)
            .map(|_| {
                let (h, n) = (p.handle(), n.clone());
                thread::spawn(move || {
                    for _ in 0..2000 {
                        let n = n.clone();
                        h.execute(move || {
                            n.fetch_add(1, SeqCst);
                        })
                        .unwrap();
                    }
                })
            })
            .collect();
        for t in producers {
            t.join().unwrap();
        }
        p.join();
        assert_eq!(n.load(SeqCst), 16_000, "{s:?}");
    }
}

#[test]
fn worker_threads_are_named() {
    let p = ThreadPool::builder().threads(1).name("pool-x").build().unwrap();
    let name = p.spawn(|| thread::current().name().map(str::to_owned)).join().unwrap();
    // An outside thread that joins does not run tasks itself.
    assert_eq!(name.as_deref(), Some("pool-x-0"));
}
