use std::fs;

pub struct TestHelper;

impl TestHelper {
    pub fn cleanup(working_dir: &str) {
        let _ = fs::remove_dir_all(working_dir);
    }
}