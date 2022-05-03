use std::fs;

#[allow(dead_code)]
pub struct TestHelper;

impl TestHelper {
    #[allow(dead_code)]
    pub fn remove_dir(working_dir: &str) {
        let _ = fs::remove_dir_all(working_dir);
    }
}