use std::fmt;

#[derive(Debug)]
pub struct AppError {
    message: String,
}

impl AppError {
    pub fn new(path: &str, method: &str, tag: &str, err: &str) -> Self {
        AppError {
            message: [&chrono::Utc::now().to_string(), path, method, tag, err].join(" | "),
        }
    }

    pub fn show(&self) {
        println!("{}", self.message);
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}
