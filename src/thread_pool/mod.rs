use std::io::Result;

pub trait ThreadPool {
    fn new(thread_count: u16) -> Result<Self> where Self: Sized;
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}

pub mod shared_queue_thread_pool;
pub mod naive_thread_pool;
pub mod rayon_thread_pool;