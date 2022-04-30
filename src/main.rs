use clap::Parser;

pub(crate) mod models;
use models::{account::Account, error::AppError, transactions::Transactions};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// csv to parse
    file: String,
}

fn main() -> Result<(), AppError> {
    {
        // // set up logging
        // AppError::init_logging()?;

        // // process csv into client directory files
        // let args = Args::parse();
        // Transactions::group_txns_by_client(&args.file)?;
    }

    // process client files in to summary files & output
    Account::merge_txns_by_client()?;
    Ok(())
}
