use clap::Parser;

pub(crate) mod models;
use models::{account::Account, error::AppError, transactions::Transactions};

// todo: add regex to look for only .csv files
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// csv to parse
    file: String,
}

fn main() -> Result<(), AppError> {
    {
        // set up logging
        AppError::init_logging()?;

        // process csv into client directory files
        // let args = Args::parse();
        // let transactions = Transactions::new();
        // transactions.linear_group_txns_by_client(&args.file)?;
    }

    // process client files in to summary files & output
    let account = Account::new();
    account.merge_txns()?;
    Ok(())
}
