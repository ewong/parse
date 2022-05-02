use std::fs;

use crate::lib::error::AppError;

const PATH: &str = "model/tx_summary";

pub trait TxSummaryData: Send + Sync + 'static {
    fn client_id(&self) -> u16;
    fn dir_path(&self) -> &str;
}

pub struct TxSummary {
    client_id: u16,
    dir_path: String,
}

impl TxSummary {
    pub fn summaries(working_dir: &str) -> Result<Vec<TxSummary>, AppError> {
        let paths = fs::read_dir(working_dir)
            .map_err(|e| AppError::new(PATH, "summaries", "00", &e.to_string()))?;

        let summaries: Vec<TxSummary> = paths
            .map(|e| {
                if let Ok(path) = e {
                    if path.path().is_dir() {
                        let dir_path = path.path().display().to_string();
                        let pos = dir_path.rfind("/").unwrap();
                        let client_id: u16 = dir_path[(pos + 1)..].parse::<u16>().unwrap();
                        return TxSummary {
                            client_id,
                            dir_path,
                        };
                    }
                }
                TxSummary {
                    client_id: 0,
                    dir_path: "".to_string(),
                }
            })
            .filter(|s| s.dir_path.len() > 0)
            .collect();

        Ok(summaries)
    }
}

impl TxSummaryData for TxSummary {
    fn client_id(&self) -> u16 {
        self.client_id
    }

    fn dir_path(&self) -> &str {
        &self.dir_path
    }
}
