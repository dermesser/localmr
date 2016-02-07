use std::collections::BTreeMap;
use std::io::BufWriter;
use std::vec;

use record_types::*;
use formats::util::MRSinkGenerator;

pub struct BufWriterSinkGen {
    // bogus field so the struct isn't empty
    i: i32,
}

impl MRSinkGenerator for BufWriterSinkGen {
    type Sink = Vec<u8>;
    fn new_output(&mut self, name: &String) -> Self::Sink {
        Vec::new()
    }
}
