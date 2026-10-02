//! Compares workpool with spawning a thread per task and with rayon.
//!
//!     cargo run --release --example bench -- [--threads N] [--runs N]
//!
//! Every number is the median of `--runs` runs after one warm-up run.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use workpool::{Handle, Strategy, ThreadPool};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn measure(runs: usize, mut f: impl FnMut()) -> f64 {
    f(); // warm-up
    median(
        (0..runs)
            .map(|_| {
                let t = Instant::now();
                f();
                t.elapsed().as_secs_f64()
            })
            .collect(),
    )
}

fn fib_seq(n: u64) -> u64 {
    if n < 2 {
        n
    } else {
        fib_seq(n - 1) + fib_seq(n - 2)
    }
}

fn fib_workpool(h: &Handle, n: u64, cutoff: u64) -> u64 {
    if n <= cutoff {
        return fib_seq(n);
    }
    let (mut a, mut b) = (0, 0);
    h.scope(|s| {
        s.spawn(|| a = fib_workpool(h, n - 1, cutoff));
        s.spawn(|| b = fib_workpool(h, n - 2, cutoff));
    });
    a + b
}

fn fib_rayon(n: u64, cutoff: u64) -> u64 {
    if n <= cutoff {
        return fib_seq(n);
    }
    let (a, b) = rayon::join(|| fib_rayon(n - 1, cutoff), || fib_rayon(n - 2, cutoff));
    a + b
}

fn fmt_rate(tasks: u64, secs: f64) -> String {
    format!("{:>8.2} M tasks/s", tasks as f64 / secs / 1e6)
}

fn main() {
    let mut threads = std::thread::available_parallelism().map_or(4, |n| n.get());
    let mut runs = 7;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--threads" => threads = args.next().and_then(|v| v.parse().ok()).expect("--threads N"),
            "--runs" => runs = args.next().and_then(|v| v.parse().ok()).expect("--runs N"),
            _ => panic!("unknown argument {a}"),
        }
    }

    let shared = ThreadPool::builder().threads(threads).strategy(Strategy::Shared).build().unwrap();
    let stealing = ThreadPool::builder().threads(threads).strategy(Strategy::WorkStealing).build().unwrap();
    let rayon_pool = rayon::ThreadPoolBuilder::new().num_threads(threads).build().unwrap();

    println!("workers: {threads}, runs per figure: {runs} (median)\n");

    // 1. Many tiny tasks submitted from one thread.
    const N: u64 = 1_000_000;
    println!("1. {N} tiny tasks (one atomic add each), submitted from one thread, until all have run");
    for (name, pool) in [("workpool shared queue", &shared), ("workpool work stealing", &stealing)] {
        let secs = measure(runs, || {
            for _ in 0..N {
                pool.execute(|| {
                    COUNTER.fetch_add(1, Relaxed);
                })
                .unwrap();
            }
            pool.join();
        });
        println!("   {name:<24}{}   {:>8.1} ms", fmt_rate(N, secs), secs * 1e3);
    }
    let secs = measure(runs, || {
        rayon_pool.scope(|s| {
            for _ in 0..N {
                s.spawn(|_| {
                    COUNTER.fetch_add(1, Relaxed);
                });
            }
        });
    });
    println!("   {:<24}{}   {:>8.1} ms", "rayon scope", fmt_rate(N, secs), secs * 1e3);
    const T: u64 = 20_000;
    let secs = measure(3.min(runs), || {
        let hs: Vec<_> = (0..T)
            .map(|_| {
                std::thread::spawn(|| {
                    COUNTER.fetch_add(1, Relaxed);
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
    });
    println!("   {:<24}{}   {:>8.1} ms  ({T} tasks)", "thread per task", fmt_rate(T, secs), secs * 1e3);

    // 2. Recursive fork-join: tasks spawned from inside tasks.
    let (n, cutoff) = (38u64, 24u64);
    let expect = fib_seq(std::hint::black_box(n));
    println!("\n2. fork-join fib({n}), sequential below fib({cutoff}); tasks spawn tasks");
    let secs_seq = measure(3.min(runs), || assert_eq!(fib_seq(std::hint::black_box(n)), expect));
    println!("   {:<24}{:>8.1} ms", "single thread", secs_seq * 1e3);
    for (name, pool) in [("workpool shared queue", &shared), ("workpool work stealing", &stealing)] {
        let h = pool.handle();
        let secs = measure(runs, || assert_eq!(fib_workpool(&h, n, cutoff), expect));
        println!("   {name:<24}{:>8.1} ms   {:>5.1}x single thread", secs * 1e3, secs_seq / secs);
    }
    let secs = measure(runs, || assert_eq!(rayon_pool.install(|| fib_rayon(n, cutoff)), expect));
    println!("   {:<24}{:>8.1} ms   {:>5.1}x single thread", "rayon join", secs * 1e3, secs_seq / secs);

    // 3. Time from submitting to an idle pool until the task starts.
    println!("\n3. wake-up latency: submit one task to an idle pool, time until it starts (1000 samples)");
    let sample = |submit: &mut dyn FnMut() -> Instant| -> (f64, f64) {
        let mut v = Vec::new();
        for _ in 0..1000 {
            std::thread::sleep(Duration::from_micros(300)); // let workers go back to sleep
            let t = Instant::now();
            let started = submit();
            v.push(started.duration_since(t).as_secs_f64() * 1e6);
        }
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        (v[v.len() / 2], v[v.len() * 99 / 100])
    };
    for (name, pool) in [("workpool shared queue", &shared), ("workpool work stealing", &stealing)] {
        let (p50, p99) = sample(&mut || pool.spawn(Instant::now).join().unwrap());
        println!("   {name:<24}p50 {p50:>6.1} us   p99 {p99:>7.1} us");
    }
    let (p50, p99) = sample(&mut || {
        let (tx, rx) = mpsc::channel();
        rayon_pool.spawn(move || tx.send(Instant::now()).unwrap());
        rx.recv().unwrap()
    });
    println!("   {:<24}p50 {p50:>6.1} us   p99 {p99:>7.1} us", "rayon spawn");
    let (p50, p99) = sample(&mut || std::thread::spawn(Instant::now).join().unwrap());
    println!("   {:<24}p50 {p50:>6.1} us   p99 {p99:>7.1} us", "thread spawn");

    let s = stealing.stats();
    println!("\nwork stealing pool totals: {} tasks run, {} stolen", s.executed, s.stolen);
}
