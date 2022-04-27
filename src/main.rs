use clap::Parser;

pub(crate) mod models;

use models::chunk::Chunk;

// todo: add regex to look for only .csv files
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// csv to parse
    file: String,
}

fn main() {
    // process csv into client directory files
    let args = Args::parse();
    let chunk = Chunk::new(&args.file);
    let res = chunk.process_csv();
    if let Err(err) = res {
        print!("{}", &err.msg);
    }
}
