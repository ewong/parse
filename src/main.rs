use clap::Parser;

pub(crate) mod models;
pub(crate) mod traits;

use models::{error::AppError, processor::Processor};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// csv to parse
    file: String,
}

fn main() -> Result<(), AppError> {
    let args = Args::parse();
    let p = Processor::new(&args.file);
    p.process_csv()?;
    Ok(())
}
