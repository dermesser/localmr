#![allow(dead_code)]

use formats::util::MRSinkGenerator;
use formats::lines::LinesWriter;

pub struct BufWriterSinkGen {
    // bogus field so the struct isn't empty
    i: i32,
}

impl MRSinkGenerator for BufWriterSinkGen {
    type Sink = LinesWriter<Vec<u8>>;
    fn new_output(&mut self, _: &String) -> Self::Sink {
        LinesWriter::new_to_write(Vec::new())
    }
}
