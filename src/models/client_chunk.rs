use std::fs::File;
use std::sync::mpsc;
use std::thread;

use super::app_error::AppError;

const PATH: &str = "model/chunk";
const FN_PROCESS_CSV: &str = "process_csv";

/*
todo:
- use tx.clone() when switching to multi threaded chunking
- split paraellel chunking using iterator

considerations
- check memory usage & chunk accordingly
- fail out gracefully after
- allow resuming afterwards

test cases
- malformed type, client, tx, or amount
- invalid number of fields
*/

pub struct ClientChunk<'a> {
    file_path: &'a str, // might not need this
}

impl<'a> ClientChunk<'a> {
    pub fn new(file_path: &'a str) -> Self {
        Self { file_path }
    }

    pub fn process_csv(&self) -> Result<(), AppError> {
        let (tx, rx) = mpsc::channel();

        let path = self.file_path.to_string();
        thread::spawn(move || {
            let res = File::open(&path);
            if res.is_err() {
                let e = AppError::new(PATH, FN_PROCESS_CSV, "00", &res.err().unwrap().to_string());
                tx.send(e.msg).unwrap();
                return;
            }

            let f = res.unwrap();
            let mut rdr = csv::Reader::from_reader(f);
            for res in rdr.records() {
                if res.is_err() {
                    let e =
                        AppError::new(PATH, FN_PROCESS_CSV, "01", &res.err().unwrap().to_string());
                    tx.send(e.msg).unwrap();
                    return;
                }
                println!("{:?}", res.unwrap());
            }
        });

        for received in rx {
            print!("{}", received);
        }

        Ok(())
    }
}
