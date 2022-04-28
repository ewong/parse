// use std::fs;
// use std::time::Instant;

// use super::csv::TxRow;
// use super::error::AppError;

// const PATH: &str = "models/account";
// const INPUT_DIR: &str = "data/transactions";
// const OUTPUT_DIR: &str = "data/accounts";

// const FN_MERGE_TXNS: &str = "merge_txns";

// /*
// todo:
// - get previous summary as a starting point
// */

// pub struct Account {}

// impl Account {
//     pub fn new() -> Self {
//         Self {}
//     }

//     pub fn merge_txns(&self) -> Result<(), AppError> {
//         let start = Instant::now();
//         let paths = fs::read_dir(INPUT_DIR)
//             .map_err(|e| AppError::new(PATH, FN_MERGE_TXNS, "00", &e.to_string()))?;

//         let pool = rayon::ThreadPoolBuilder::new()
//             .num_threads(4)
//             .build()
//             .unwrap();

//         for pres in paths {
//             pool.spawn(move || {
//                 if pres.is_err() {
//                     println!("dead0");
//                     return;
//                 }

//                 let res = pres.unwrap().path();
//                 if !res.is_dir() {
//                     println!("dead1");
//                     return;
//                 }

//                 let opt = res.to_str();
//                 if opt.is_none() {
//                     println!("dead2");
//                     return;
//                 }

//                 let dir = opt.unwrap().to_string();
//                 let res = fs::read_dir(&dir);
//                 if res.is_err() {
//                     println!("dead3");
//                     return;
//                 }
//                 // println!("{}", dir);

//                 let mut tx_row = TxRow::new();
//                 let mut count = 0.0;

//                 for file_path in res.unwrap() {
//                     if file_path.is_err() {
//                         println!("dead4");
//                         continue;
//                     }
//                     let fp = [&dir, file_path.unwrap().file_name().to_str().unwrap()].join("/");
//                     let f = fs::File::open(&fp).unwrap();

//                     let mut rdr = csv::Reader::from_reader(f);
//                     for rdrres in rdr.deserialize() {
//                         let row: TxRow = rdrres.unwrap();
//                         if tx_row.client_id == 0 {
//                             tx_row.type_id = row.type_id;
//                             tx_row.client_id = row.client_id;
//                         }

//                         tx_row.tx_id += row.tx_id;
//                         count += 1.0;
//                     }
//                 }
//                 tx_row.tx_id = tx_row.tx_id / count;
//                 println!("{:?}, count: {}", tx_row, count);

//                 // write to file
//             });
//         }

//         let duration = start.elapsed();
//         println!("Time elapsed in expensive_function() is: {:?}", duration);

//         Ok(())
//     }
// }
