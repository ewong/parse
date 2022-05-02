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
    let p = Processor::new(&args.file);
    if let Err(err) = p.process_csv(true) {
        err.show();
    }
}
