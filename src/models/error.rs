#[derive(Debug)]
pub struct ModelError {
    pub msg: String,
}

impl ModelError {
    pub fn new(path: &str, method: &str, tag: &str, err: &str) -> Self {
        let log_msg = [path, method, tag, err].join(" | ");
        println!("{}", log_msg);
        ModelError {
            msg: err.to_string(),
        }
    }
}
