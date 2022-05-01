use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct TxMergeInputRow<'a> {
    #[serde(rename(deserialize = "type", serialize = "type"))]
    pub type_id: &'a [u8],
    #[serde(rename(deserialize = "client", serialize = "client"))]
    pub client_id: u16,
    #[serde(rename(deserialize = "tx", serialize = "tx"))]
    pub tx_id: f32,
    #[serde(rename(deserialize = "amount", serialize = "amount"))]
    pub amount: Option<f64>,
}

impl<'a> TxMergeInputRow<'a> {
    pub fn new() -> Self {
        Self {
            type_id: b"",
            client_id: 0,
            tx_id: 0.0,
            amount: None,
        }
    }
}

