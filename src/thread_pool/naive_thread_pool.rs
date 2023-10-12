use super::ThreadPool;

struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_: u16) -> Self {
        NaiveThreadPool { }
    }

    ///
    /// Naive implementation for spawn() which merely creates a new thread per
    /// job added to the thread pool. Job must abide by Send + 'static
    /// trait bounds
    /// 
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static {
        std::thread::spawn(|| {
            job();
        });
    }
}