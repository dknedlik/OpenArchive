use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ShutdownToken {
    inner: Arc<AtomicBool>,
}

impl ShutdownToken {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn signal(&self) {
        self.inner.store(true, Ordering::SeqCst);
    }

    pub fn is_shutdown(&self) -> bool {
        self.inner.load(Ordering::SeqCst)
    }
}

impl Default for ShutdownToken {
    fn default() -> Self {
        Self::new()
    }
}
