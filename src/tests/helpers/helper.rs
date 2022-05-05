use std::fs;

#[allow(dead_code)]
pub struct TestHelper;

impl TestHelper {
    #[allow(dead_code)]
    pub fn remove_dir(dir: &str) {
        let _ = fs::remove_dir_all(dir);
    }

    #[allow(dead_code)]
    pub fn remove_file(file: &str) {
        let _ = fs::remove_file(file);
    }
}
