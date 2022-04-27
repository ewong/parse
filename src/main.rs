use clap::Parser;

pub(crate) mod models;
use models::{app_error::AppError, client_chunk::ClientChunk};

// todo: add regex to look for only .csv files
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// csv to parse
    file: String,
}

fn main() -> Result<(), AppError> {
    // set up logging
    AppError::init_logging()?;

    // process csv into client directory files
    let args = Args::parse();
    let chunk = ClientChunk::new(&args.file);
    chunk.process_csv()?;

    Ok(())
}
