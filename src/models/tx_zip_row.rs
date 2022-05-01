use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct TxZipInputRow<'a> {
    #[serde(rename(deserialize = "type"))]
    pub type_id: &'a [u8],
    #[serde(rename(deserialize = "client"))]
    pub client_id: u16,
    #[serde(rename(deserialize = "tx"))]
    pub tx_id: f32,
    #[serde(rename(deserialize = "amount"))]
    pub amount: Option<f64>,
}

impl<'a> TxZipInputRow<'a> {
    pub fn new() -> Self {
        Self {
            type_id: b"",
            client_id: 0,
            tx_id: 0.0,
            amount: None,
        }
    }
}
