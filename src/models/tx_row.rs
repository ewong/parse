use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TxRow {
    #[serde(rename(deserialize = "userId", serialize = "userId"))]
    pub type_id: String,
    #[serde(rename(deserialize = "movieId", serialize = "movieId"))]
    pub client_id: u32,
    #[serde(rename(deserialize = "rating", serialize = "rating"))]
    pub tx_id: f32,
    #[serde(rename(deserialize = "timestamp", serialize = "timestamp"))]
    pub amount: Option<u32>,
}

impl TxRow {
    pub fn new() -> Self {
        Self {
            type_id: "".to_string(),
            client_id: 0,
            tx_id: 0.0,
            amount: None,
        }
    }
}
