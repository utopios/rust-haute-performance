use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Instant;


#[derive(Debug, PartialEq, Eq)]
pub enum BufferError {
    Closed,
}


struct Inner<T> {
    queue: VecDeque<T>,
    capacity: usize,
    closed: bool,
}

pub struct RingBuffer<T> {
    inner: Mutex<Inner<T>>,
    not_full: Condvar,
    not_empty: Condvar,
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity doit être > 0");
        Self {
            inner: Mutex::new(Inner {
                queue: VecDeque::with_capacity(capacity),
                capacity,
                closed: false,
            }),
            not_full: Condvar::new(),
            not_empty: Condvar::new(),
        }
    }

    pub fn push(&self, value: T) -> Result<(), BufferError> {
        let mut guard = self.inner.lock().unwrap();
        while guard.queue.len() == guard.capacity && !guard.closed {
            guard = self.not_full.wait(guard).unwrap();
        }
        if guard.closed {
            return Err(BufferError::Closed);
        }
        guard.queue.push_back(value);
        drop(guard);
        self.not_empty.notify_one();
        Ok(())
    }

    pub fn pop(&self) -> Result<T, BufferError> {
        let mut guard = self.inner.lock().unwrap();
        while guard.queue.is_empty() && !guard.closed {
            guard = self.not_empty.wait(guard).unwrap();
        }
        if let Some(v) = guard.queue.pop_front() {
            drop(guard);
            self.not_full.notify_one();
            return Ok(v);
        }
        Err(BufferError::Closed)
    }

    pub fn try_push(&self, value: T) -> Result<(), BufferError> {
        let mut guard = self.inner.lock().unwrap();
        if guard.closed {
            return Err(BufferError::Closed);
        }
        if guard.queue.len() == guard.capacity {
            return Err(BufferError::Closed);
        }
        guard.queue.push_back(value);
        drop(guard);
        self.not_empty.notify_one();
        Ok(())
    }

    pub fn try_pop(&self) -> Result<Option<T>, BufferError> {
        let mut guard = self.inner.lock().unwrap();
        if let Some(v) = guard.queue.pop_front() {
            drop(guard);
            self.not_full.notify_one();
            return Ok(Some(v));
        }
        if guard.closed {
            Err(BufferError::Closed)
        } else {
            Ok(None)
        }
    }

    pub fn close(&self) {
        let mut guard = self.inner.lock().unwrap();
        guard.closed = true;
        drop(guard);
        // notify_all : tous les threads bloqués doivent recheck closed.
        self.not_full.notify_all();
        self.not_empty.notify_all();
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().queue.len()
    }

    pub fn is_closed(&self) -> bool {
        self.inner.lock().unwrap().closed
    }
}


fn demo_fonctionnelle() {
    println!("--- [1] Test fonctionnel : 4 producers × 4 consumers ---");
    let buf = Arc::new(RingBuffer::<u64>::new(128));
    let n_prod = 4;
    let n_cons = 4;
    let messages_par_prod = 1_000;

    let producers: Vec<_> = (0..n_prod)
        .map(|p| {
            let b = Arc::clone(&buf);
            thread::spawn(move || {
                for i in 0..messages_par_prod {
                    let v = (p * messages_par_prod + i) as u64;
                    b.push(v).expect("push ne devrait pas échouer");
                }
            })
        })
        .collect();

    let consumers: Vec<_> = (0..n_cons)
        .map(|_| {
            let b = Arc::clone(&buf);
            thread::spawn(move || {
                let mut local = 0u64;
                let mut count = 0usize;
                loop {
                    match b.pop() {
                        Ok(v) => {
                            local += v;
                            count += 1;
                        }
                        Err(BufferError::Closed) => break,
                    }
                }
                (local, count)
            })
        })
        .collect();

    for p in producers {
        p.join().unwrap();
    }
    buf.close();

    let mut total = 0u64;
    let mut total_count = 0usize;
    for c in consumers {
        let (s, n) = c.join().unwrap();
        total += s;
        total_count += n;
    }
    let attendu: u64 = (0..(n_prod * messages_par_prod))
        .map(|i| i as u64)
        .sum();
    assert_eq!(total, attendu, "somme incorrecte");
    assert_eq!(total_count, n_prod * messages_par_prod);
    println!(
        "  ✓ {n_prod} producers × {n_cons} consumers : somme = {total}, count = {total_count}\n"
    );
}

fn bench_contention(n_prod: usize, n_cons: usize, total_messages: usize) -> u128 {
    let messages_par_prod = total_messages / n_prod;
    let buf = Arc::new(RingBuffer::<u64>::new(1024));
    let start = Instant::now();

    let producers: Vec<_> = (0..n_prod)
        .map(|p| {
            let b = Arc::clone(&buf);
            thread::spawn(move || {
                for i in 0..messages_par_prod {
                    let _ = b.push((p * messages_par_prod + i) as u64);
                }
            })
        })
        .collect();

    let consumers: Vec<_> = (0..n_cons)
        .map(|_| {
            let b = Arc::clone(&buf);
            thread::spawn(move || {
                let mut count = 0usize;
                while b.pop().is_ok() {
                    count += 1;
                }
                count
            })
        })
        .collect();

    for p in producers {
        p.join().unwrap();
    }
    buf.close();
    for c in consumers {
        let _ = c.join().unwrap();
    }
    start.elapsed().as_millis()
}

fn main() {
    println!("=== Correction Exercice 5 : Ring buffer thread-safe ===\n");
    demo_fonctionnelle();

    println!("--- [2] Bench de contention (100 000 messages total) ---");
    for &(p, c) in &[(1usize, 1usize), (4, 4), (8, 8)] {
        let t = bench_contention(p, c, 100_000);
        let throughput = 100_000 as f64 / t as f64;
        println!(
            "  {p} producers × {c} consumers : {t} ms ({throughput:.0} K msg/s)"
        );
    }
}

