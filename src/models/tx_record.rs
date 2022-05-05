use csv::ByteRecord;
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};
use std::str;

use crate::lib::constants::{AMOUNT_POS, CLIENT_POS, TX_POS, TYPE_POS};

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

    pub fn header_type(record: &ByteRecord) -> bool {
        if &record[TYPE_POS] == b"type"
            || &record[CLIENT_POS] == b"client"
            || &record[TX_POS] == b"tx"
            || &record[AMOUNT_POS] == b"amount"
        {
            return true;
        }

        for i in TYPE_POS..AMOUNT_POS {
            if let Ok(string) = str::from_utf8(&record[i]) {
                let s = string.to_lowercase().replace(" ", "");
                if s == "type" || s == "client" || s == "tx" || s == "amount" {
                    return true;
                }
            }
        }

        return false;
    }

    // pub fn as_binary(&self) -> &[u8] {
    //     match self {
    //         Self::DEPOSIT => B_DEPOSIT,
    //         Self::WITHDRAW => B_WITHDRAW,
    //         Self::DISPUTE => B_DISPUTE,
    //         Self::RESOLVE => B_RESOLVE,
    //         Self::CHARGEBACK => B_CHARGEBACK,
    //         _ => b"",
    //     }
    // }

    // pub fn as_u8(&self) -> u8 {
    //     match self {
    //         Self::DEPOSIT => 0,
    //         Self::WITHDRAW => 1,
    //         Self::DISPUTE => 2,
    //         Self::RESOLVE => 3,
    //         Self::CHARGEBACK => 4,
    //         _ => 5,
    //     }
    // }

    pub fn to_string(&self) -> String {
        match self {
            Self::DEPOSIT => "deposit".to_string(),
            Self::WITHDRAW => "withdraw".to_string(),
            Self::DISPUTE => "dispute".to_string(),
            Self::RESOLVE => "resolve".to_string(),
            Self::CHARGEBACK => "chargeback".to_string(),
            _ => "none".to_string(),
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

#[derive(Debug, Clone, Copy)]
pub struct TxRow {
    pub type_id: TxRecordType,
    pub client_id: u16,
    pub tx_id: u32,
    pub amount: Decimal,
    pub conflict_type_id: TxRecordType,
}

impl TxRow {
    pub fn new_from_string(string: &str) -> Self {
        let a: Vec<&str> = string.split(",").collect();
        let conflict_type_id: TxRecordType;
        if a.len() < 5 {
            conflict_type_id = TxRecordType::NONE;
        } else {
            conflict_type_id = TxRecordType::from_binary(a[4].as_bytes());
        }
        let type_id = TxRecordType::from_binary(a[0].as_bytes());
        let client_id = a[1].parse::<u16>().unwrap();
        let tx_id = a[2].parse::<u32>().unwrap();
        let amount = Decimal::from_str(a[3]).unwrap();
        Self {
            type_id,
            client_id,
            tx_id,
            amount,
            conflict_type_id,
        }
    }

    pub fn to_string(
        tx_type: &TxRecordType,
        client_id: &u16,
        tx_id: &u32,
        amount: &Decimal,
    ) -> String {
        format!(
            "{},{},{},{}",
            tx_type.to_string(),
            client_id.to_string(),
            tx_id.to_string(),
            format!("{:.4}", amount)
        )
    }
}
