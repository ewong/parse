use csv::Reader;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use std::str;

const PATH: &str = "models/tx_record";

const TYPE_POS: usize = 0;

const TYPE_COL: &str = "type";
const CLIENT_COL: &str = "client";
const TX_COL: &str = "tx";
const AMOUNT_COL: &str = "amount";

const FN_WRITE_RECORDS: &str = "writer_records";
const FN_WRITE_CONFLICTS: &str = "write_conflicts";

const B_DEPOSIT: &[u8] = b"deposit";
const B_WITHDRAW: &[u8] = b"withdraw";
const B_DISPUTE: &[u8] = b"dispute";
const B_RESOLVE: &[u8] = b"resolve";
const B_CHARGEBACK: &[u8] = b"chargeback";

#[derive(PartialEq, Debug, Clone, Copy)]
pub enum TxRecordType {
    DEPOSIT = 0,
    WITHDRAW,
    DISPUTE,
    RESOLVE,
    CHARGEBACK,
    NONE,
}

impl TxRecordType {
    pub fn from_binary(binary: &[u8]) -> Self {
        let initial_type = match binary {
            B_DEPOSIT => Self::DEPOSIT,
            B_WITHDRAW => Self::WITHDRAW,
            B_DISPUTE => Self::DISPUTE,
            B_RESOLVE => Self::RESOLVE,
            B_CHARGEBACK => Self::CHARGEBACK,
            _ => Self::NONE,
        };

        if initial_type != Self::NONE {
            return initial_type;
        }

        // try to convert it to a lower case str + retest
        if let Ok(str) = str::from_utf8(binary) {
            return match str.to_lowercase().replace(" ", "").as_str() {
                "deposit" => Self::DEPOSIT,
                "withdraw" => Self::WITHDRAW,
                "dispute" => Self::DISPUTE,
                "resolve" => Self::RESOLVE,
                "chargeback" => Self::CHARGEBACK,
                _ => Self::NONE,
            };
        }

        // return none
        Self::NONE
    }

    pub fn as_binary(&self) -> &[u8] {
        match self {
            Self::DEPOSIT => B_DEPOSIT,
            Self::WITHDRAW => B_WITHDRAW,
            Self::DISPUTE => B_DISPUTE,
            Self::RESOLVE => B_RESOLVE,
            Self::CHARGEBACK => B_CHARGEBACK,
            _ => b"",
        }
    }

    pub fn as_u8(&self) -> u8 {
        match self {
            Self::DEPOSIT => 0,
            Self::WITHDRAW => 1,
            Self::DISPUTE => 2,
            Self::RESOLVE => 3,
            Self::CHARGEBACK => 4,
            _ => 5,
        }
    }

    pub fn conflict_type(&self) -> bool {
        *self == Self::DISPUTE || *self == Self::RESOLVE || *self == Self::CHARGEBACK
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
pub struct TxRecord<'a> {
    #[serde(rename(deserialize = "type", serialize = "type"))]
    pub type_id: &'a [u8],
    #[serde(rename(deserialize = "client", serialize = "client"))]
    pub client_id: u16,
    #[serde(rename(deserialize = "tx", serialize = "tx"))]
    pub tx_id: u32,
    #[serde(
        rename(deserialize = "amount", serialize = "amount"),
        with = "rust_decimal::serde::str"
    )]
    pub amount: Decimal,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct TxRecordSmall<'a> {
    #[serde(rename(deserialize = "type", serialize = "type"))]
    pub type_id: &'a [u8],
    #[serde(rename(deserialize = "client", serialize = "client"))]
    pub client_id: u16,
    #[serde(rename(deserialize = "tx", serialize = "tx"))]
    pub tx_id: u32,
}

// #[derive(Debug, Clone, Copy)]
// pub struct TxRow {
//     pub type_id: u8,
//     pub client_id: u16,
//     pub tx_id: u32,
//     pub amount: Decimal,
// }

// impl TxRow {
//     pub fn new() -> Self {
//         Self {
//             type_id: 0,
//             client_id: 0,
//             tx_id: 0,
//             amount: Decimal::new(0, 0),
//         }
//     }

//     pub fn set(&mut self, type_id: u8, client_id: u16, tx_id: u32, amount: Decimal) {
//         self.type_id = type_id;
//         self.client_id = client_id;
//         self.tx_id = tx_id;
//         self.amount = amount;
//     }
// }