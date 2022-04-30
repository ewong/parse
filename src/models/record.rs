pub const TYPE_POS: usize = 0;
pub const CLIENT_POS: usize = 1;
pub const TX_POS: usize = 2;
pub const AMOUNT_POS: usize = 3;

const TYPE_DEPOSIT: &[u8] = b"deposit";
const TYPE_WITHDRAW: &[u8] = b"withdraw";
const TYPE_DISPUTE: &[u8] = b"dispute";
const TYPE_RESOLVE: &[u8] = b"resolve";
const TYPE_CHARGEBACK: &[u8] = b"chargeback";

pub trait Record {
    fn type_id(&self) -> &[u8];

    fn valid_record(&self) -> bool {
        // // validate type id
        // if &record[TYPE_POS] == TYPE_DEPOSIT || &record[TYPE_POS] == TYPE_WITHDRAW {
        //     println!("capture tx id ({:?}) in a separate file", &record[TX_POS]);
        // } else if &record[TYPE_POS] == TYPE_DISPUTE
        //     || &record[TYPE_POS] == TYPE_RESOLVE
        //     || &record[TYPE_POS] == TYPE_CHARGEBACK
        // {
        //     println!("write tx id ({:?}) in a separate file", &record[TX_POS]);
        // } else {
        //     println!(
        //         "write client id ({:?}) in tx_error.csv",
        //         &record[CLIENT_POS]
        //     );
        // }
        return true;
    }
}