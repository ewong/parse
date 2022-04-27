use log::LevelFilter;
use log4rs::append::file::FileAppender;
use log4rs::config::{Appender, Config, Root};
use log4rs::encode::pattern::PatternEncoder;

const PATH: &str = "models/error";
const FN_INIT_LOG: &str = "init_log";

#[derive(Debug)]
pub struct AppError {
    pub msg: String,
}

impl AppError {
    pub fn new(path: &str, method: &str, tag: &str, err: &str) -> Self {
        log::error!("{} | {} | {} | {}", path, method, tag, err);
        AppError {
            msg: err.to_string(),
        }
    }

    pub fn init_logging() -> Result<(), AppError> {
        let logfile = FileAppender::builder()
            .encoder(Box::new(PatternEncoder::new("{l} - {m}\n")))
            .build("error.log")
            .map_err(|e| AppError::new(PATH, FN_INIT_LOG, "00", &e.to_string()))?;

        let config = Config::builder()
            .appender(Appender::builder().build("logfile", Box::new(logfile)))
            .build(Root::builder().appender("logfile").build(LevelFilter::Info))
            .map_err(|e| AppError::new(PATH, FN_INIT_LOG, "01", &e.to_string()))?;

        log4rs::init_config(config)
            .map_err(|e| AppError::new(PATH, FN_INIT_LOG, "02", &e.to_string()))?;

        Ok(())
    }
}
