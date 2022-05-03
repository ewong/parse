use clap::Parser;

pub(crate) mod lib;
pub(crate) mod models;
pub(crate) mod tests;

use models::processor::Processor;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// csv to parse
    file: String,
}

fn main() {
    let args = Args::parse();
    let result = Processor::new(&args.file);
    if result.is_err() {
        result.err().unwrap().show();
        return;
    }

    let p = result.unwrap();
    if let Err(err) = p.process_csv(false) {
        err.show();
    }
}
