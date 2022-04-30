pub const OUTPUT_ROOT_DIR: &str = "data";
pub const ACCOUNT_DIR: &str = "data/accounts";

pub const TYPE_POS: usize = 0;
pub const CLIENT_POS: usize = 1;
pub const TX_POS: usize = 2;
pub const AMOUNT_POS: usize = 3;

pub const TYPE_DEPOSIT: &[u8] = b"deposit";
pub const TYPE_WITHDRAW: &[u8] = b"withdraw";
pub const TYPE_DISPUTE: &[u8] = b"dispute";
pub const TYPE_RESOLVE: &[u8] = b"resolve";
pub const TYPE_CHARGEBACK: &[u8] = b"chargeback";

