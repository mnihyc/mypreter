use std::{future::Future, ops::AddAssign, pin::Pin, sync::{atomic::{AtomicUsize, Ordering}, Arc}};

pub type AsyncFuture<T> = Pin<Box<dyn Future<Output = T> + Send + 'static>>;
pub type AsyncFn<A, T> = Arc<dyn Fn(A) -> AsyncFuture<T> + Send + Sync + 'static>;

pub struct Counter {
    count: AtomicUsize,
}

impl Counter {
    pub fn new(value: usize) -> Self {
        Counter {
            count: AtomicUsize::new(value),
        }
    }

    /// Increment the counter by 1 and return the old value
    pub fn increment(&self) -> usize {
        self.count.fetch_add(1, Ordering::Relaxed)
    }

    /// Decrement the counter by 1 and return the old value
    pub fn decrement(&self) -> usize {
        self.count.fetch_sub(1, Ordering::Relaxed)
    }

    /// Get the current value of the counter
    pub fn get(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    /// Set the counter to a new value
    pub fn set(&self, value: usize) {
        self.count.store(value, Ordering::Relaxed);
    }
}

impl AddAssign<usize> for Counter {
    fn add_assign(&mut self, rhs: usize) {
        self.count.fetch_add(rhs, Ordering::Relaxed);
    }
}


pub fn get_timestamp() -> usize {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as usize
}


