use super::tx_record::TxRow;
use std::collections::HashMap;

// const PATH: &str = "model/tx_cluster";

pub struct TxCluster {
    pub tx_row_map: HashMap<u16, Vec<TxRow>>,
}

impl TxCluster {
    pub fn new() -> Self {
        Self {
            tx_row_map: HashMap::new(),
        }
    }

    pub fn add(&mut self, tx_row: TxRow) {
        if self.tx_row_map.contains_key(&tx_row.client_id) {
            self.tx_row_map
                .entry(tx_row.client_id.clone())
                .and_modify(|e| {
                    e.push(tx_row);
                });
        } else {
            let client_id = tx_row.client_id.clone();
            let mut v = Vec::new();
            v.push(tx_row);
            self.tx_row_map.insert(client_id, v);
        }
    }
}

// pub trait TxClusterPathData: Send + Sync + 'static {
//     fn client_id(&self) -> u16;
//     fn dir_path(&self) -> &str;
// }

// pub struct TxClusterPath {
//     client_id: u16,
//     dir_path: String,
// }

// impl TxClusterPath {
//     pub fn paths(cluster_dir: &str) -> Result<Vec<TxClusterPath>, AppError> {
//         let paths = fs::read_dir(cluster_dir)
//             .map_err(|e| AppError::new(PATH, "paths", "00", &e.to_string()))?;

//         let paths: Vec<TxClusterPath> = paths
//             .map(|e| {
//                 if let Ok(path) = e {
//                     if path.path().is_dir() {
//                         let dir_path = path.path().display().to_string();
//                         let pos = dir_path.rfind("/").unwrap();
//                         let client_id: u16 = dir_path[(pos + 1)..].parse::<u16>().unwrap();
//                         return TxClusterPath {
//                             client_id,
//                             dir_path,
//                         };
//                     }
//                 }
//                 TxClusterPath {
//                     client_id: 0,
//                     dir_path: "".to_string(),
//                 }
//             })
//             .filter(|s| s.dir_path.len() > 0)
//             .collect();

//         Ok(paths)
//     }
// }

// impl TxClusterPathData for TxClusterPath {
//     fn client_id(&self) -> u16 {
//         self.client_id
//     }

//     fn dir_path(&self) -> &str {
//         &self.dir_path
//     }
// }
