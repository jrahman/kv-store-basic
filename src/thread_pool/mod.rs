use std::io::Result;

pub trait ThreadPool {
    fn new(thread_count: u16) -> Self;
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}

pub mod shared_queue_thread_pool;
pub mod naive_thread_pool;