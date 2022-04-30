use std::fmt;

#[derive(Debug)]
pub struct AppError {
    message: String,
}

impl AppError {
    pub fn new(path: &str, method: &str, tag: &str, err: &str) -> Self {
        AppError {
            message: [path, method, tag, err].join(" | "),
        }
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}
