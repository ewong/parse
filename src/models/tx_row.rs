use std::str;

const TYPE_POS: usize = 0;
pub const CLIENT_POS: usize = 1; // tmp
const TX_POS: usize = 2;
const AMOUNT_POS: usize = 3;

const B_DEPOSIT: &[u8] = b"deposit";
const B_WITHDRAW: &[u8] = b"withdraw";
const B_DISPUTE: &[u8] = b"dispute";
const B_RESOLVE: &[u8] = b"resolve";
const B_CHARGEBACK: &[u8] = b"chargeback";
const B_NONE: &[u8] = b"";

#[derive(PartialEq)]
pub enum TxRowType {
    DEPOSIT = 0,
    WITHDRAW,
    DISPUTE,
    RESOLVE,
    CHARGEBACK,
    NONE,
}

impl TxRowType {
    pub fn pos() -> usize {
        TYPE_POS
    }

    pub fn from_binary(binary: &[u8]) -> Self {
        let initial_type = match binary {
            B_DEPOSIT => TxRowType::DEPOSIT,
            B_WITHDRAW => TxRowType::WITHDRAW,
            B_DISPUTE => TxRowType::DISPUTE,
            B_RESOLVE => TxRowType::RESOLVE,
            B_CHARGEBACK => TxRowType::CHARGEBACK,
            _ => TxRowType::NONE,
        };

        if initial_type != TxRowType::NONE {
            return initial_type;
        }

        // try to convert it to a lower case str + retest
        if let Ok(str) = str::from_utf8(binary) {
            return match str.to_lowercase().as_str() {
                "deposit" => TxRowType::DEPOSIT,
                "withdraw" => TxRowType::WITHDRAW,
                "dispute" => TxRowType::DISPUTE,
                "resolve" => TxRowType::RESOLVE,
                "chargeback" => TxRowType::CHARGEBACK,
                _ => TxRowType::NONE,
            };
        }

        // return none
        TxRowType::NONE
    }

    pub fn as_binary(&self) -> &[u8] {
        match self {
            DEPOSIT => B_DEPOSIT,
            WITHDRAW => B_WITHDRAW,
            DISPUTE => B_DISPUTE,
            RESOLVE => B_RESOLVE,
            CHARGEBACK => B_CHARGEBACK,
            NONE => B_NONE,
        }
    }
}

pub trait TxRow {
    fn type_id(&self) -> &[u8];
    fn client_id(&self) -> u16;
    fn tx_id(&self) -> u32;
    fn amount(&self) -> Option<f32>;
    fn record_type(&self) -> TxRowType;

    fn valid_record(&self) -> bool {
        if self.record_type() == TxRowType::NONE {
            return false;
        }

        return true;
    }
}
