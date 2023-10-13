use super::ThreadPool;

use std::io::Error;
use std::io::Result;

pub struct RayonThreadPool {
    pool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(thread_count: u16) -> Result<Self> {
        Ok(RayonThreadPool {
            pool: rayon::ThreadPoolBuilder::new()
                .num_threads(thread_count as usize)
                .build()
                .map_err(|e| Error::other(e.to_string()))?,
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool.spawn(job)
    }
}
