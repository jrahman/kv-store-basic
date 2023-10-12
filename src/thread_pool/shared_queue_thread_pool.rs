use std::{sync::Arc, thread::JoinHandle};

use super::ThreadPool;

use crossbeam::queue::SegQueue;

enum Message {
    Run(Box<dyn FnOnce() + Send + 'static>),
    Shutdown,
}
struct SharedQueueThreadPool {
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

impl ThreadPool for SharedQueueThreadPool {
    fn new(thread_count: u16) -> Self {
        let queue = Arc::new(SegQueue::new());
        let threads = (0..thread_count)
            .map(|_| {
                let q = queue.clone();
                std::thread::spawn(move || loop {
                    match q.pop() {
                        Some(Message::Run(f)) => {
                            f();
                        }
                        Some(Message::Shutdown) => {
                            return;
                        }
                        None => {
                            return;
                        }
                    }
                })
            })
            .collect();

        SharedQueueThreadPool { queue, threads }
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.queue.push(Message::Run(Box::new(job)));
    }
}
