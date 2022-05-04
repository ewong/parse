use csv::{ByteRecord, Reader, Trim};
use serde::Deserialize;
use std::fs::{self, File};

use super::error::AppError;

const PATH: &str = "model/csv_read";

pub trait CsvRead<'de, T, S>
where
    T: Deserialize<'de>,
    S: Deserialize<'de>,
{
    fn reader(&self) -> &Reader<File>;
    fn load_next_byte_record(&mut self) -> Result<(), AppError>;
    fn byte_record(&self) -> &'de ByteRecord;
    fn set_error(&mut self, error: Option<String>);
    fn is_header_row(record: &ByteRecord) -> bool;
    fn valid_record(record: &ByteRecord) -> bool;
    fn needs_alternate_deserialization(record: &ByteRecord) -> bool {
        false
    }
    fn process_alternate_deserialized_record(&mut self, deserialized_record: &S) {}
    fn process_deserialized_record(&mut self, deserialized_record: &T);

    fn new_reader(&mut self, csv_path: &str) -> Result<Reader<File>, AppError> {
        let f = fs::File::open(&csv_path)
            .map_err(|e| AppError::new(PATH, "new_reader", "00", &e.to_string()))?;
        Ok(csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .trim(Trim::All)
            .from_reader(f))
    }

    fn next_record(&mut self) -> bool {
        let result = self.reader().read_byte_record(byte_record.borrow_mut());
        if result.is_err() {
            let e = result.err().unwrap();
            self.set_error(Some(e.to_string()));
            return false;
        }

        if self.byte_record().len() == 0 {
            // end of file
            return false;
        }

        // todo: trap for blank lines
        if Self::is_header_row(self.byte_record()) {
            false;
        }

        // validate
        if !Self::valid_record(self.byte_record()) {
            return false;
        }

        if Self::needs_alternate_deserialization(&self.byte_record()) {
            let result = self.byte_record().deserialize::<S>(None);
            if result.is_err() {
                self.set_error(Some("invalid transaction record".to_string()));
                return false;
            }
            let tx_record = result.unwrap();
            self.process_alternate_deserialized_record(&tx_record);
            return true;
        }

        let result = self.byte_record().deserialize::<T>(None);
        if result.is_err() {
            let err = result.err().unwrap();
            self.set_error(Some(err.to_string()));
            return false;
        }

        self.process_deserialized_record(&result.unwrap());
        true
    }
}
