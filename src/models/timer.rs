use std::time::Instant;

pub struct Timer {
    watch: Instant,
}

impl Timer {
    pub fn start() -> Self {
        Self {
            watch: Instant::now(),
        }
    }

    pub fn stop(&self) {
        println!("Time elapsed is: {:?}", self.watch.elapsed());
    }
}
