use std::io::Result;
use std::panic::AssertUnwindSafe;
use std::sync::Mutex;
use std::{sync::Arc, thread::JoinHandle};

use super::ThreadPool;

use crossbeam::queue::SegQueue;

enum Message {
    Run(Box<dyn FnOnce() + Send + 'static>),
    Shutdown,
}
pub struct SharedQueueThreadPool {
    queue: Arc<SegQueue<Message>>,
    threads: Vec<JoinHandle<()>>,
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        let _ = (0..self.threads.len()).map(|_| self.queue.push(Message::Shutdown));
        while !self.threads.is_empty() {
            self.threads.pop().unwrap().join().unwrap();
        }
    }
}

impl SharedQueueThreadPool {
    fn thread_main(queue: Arc<SegQueue<Message>>) {
        loop {
            match queue.pop() {
                Some(Message::Run(f)) => {
                    let _ = std::panic::catch_unwind(AssertUnwindSafe(|| f()));
                }
                _ => {
                    return;
                }
            }
        }
    }
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(thread_count: u16) -> Result<Self> {
        let queue = Arc::new(SegQueue::new());
        let mut threads = Vec::new();

        for _ in 0..thread_count {
            let q = queue.clone();
            threads.push(std::thread::spawn(move || {
                SharedQueueThreadPool::thread_main(q)
            }));
        }

        Ok(SharedQueueThreadPool { queue, threads })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.queue.push(Message::Run(Box::new(job)));
    }
}
