#![allow(dead_code)]

use std::io::{Result, Write, Read};
use std::boxed::Box;
use std::io;
use std::fs;
use std::vec;
use std::string;

/// A length-prefixed record stream named for the original use case,
/// which was to write a log of all write operations to a database.
///
/// # WriteLog
/// 
/// WriteLog is a persistent data structure designed to be written to disk
/// that is a sequence of bytestring.
/// It can be read back in relatively efficiently and yields the same byte
/// strings; on disk, it is represented as records prefixed by 4 byte
/// big-endian length prefixes:
/// 
///      llllbbbbbbllllbbllllbbbbbbbbb...
/// 
/// Where l is a length byte and b are bytes of a bytestring.
/// 
/// There is a special case of WriteLogs: The length-prefixing can be turned
/// off in order to yield a better efficiency when encoding PCK files. Those
/// files are indexed by IDX files describing offset and length of single entries,
/// which is why we don't need length prefixes here.
///
pub struct WriteLogWriter {
    dest: Box<Write>,

    current_length: u64,
    records_written: u32,
}

fn encode_u32(val: u32) -> [u8; 4] {
    let mut buf: [u8; 4] = [0; 4];

    for i in 0..4 {
        buf[3 - i] = (val >> 8 * i) as u8;
    }

    buf
}

fn decode_u32(buf: [u8; 4]) -> u32 {
    let mut val: u32 = 0;

    for i in 0..4 {
        val |= (buf[3 - i] as u32) << i * 8;
    }

    val
}

impl WriteLogWriter {
    /// Return a new WriteLog that writes to dest
    pub fn new(dest: Box<Write>) -> WriteLogWriter {
        WriteLogWriter {
            dest: dest,
            current_length: 0,
            records_written: 0,
        }
    }

    /// Opens a WriteLog for writing. Truncates a file if append == false.
    pub fn new_to_file(file: &String, append: bool) -> io::Result<WriteLogWriter> {
        fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(append)
            .truncate(!append)
            .open(file)
            .map(move |f| WriteLogWriter::new(Box::new(f)))
    }

    /// Return how many (bytes,records) have been written.
    pub fn get_stats(&self) -> (u64, u32) {
        (self.current_length, self.records_written)
    }
}
impl Write for WriteLogWriter {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        // BUG: May not account the length in a correct way if the length prefix
        // is written, but not the record.
        let result = self.dest
                         .write(&encode_u32(buf.len() as u32)[0..4])
                         .and(self.dest.write(buf));
        match result {
            Err(_) => result,
            Ok(_) => {
                self.current_length += 4 + buf.len() as u64;
                self.records_written += 1;
                result
            }
        }
    }

    fn flush(&mut self) -> Result<()> {
        self.dest.flush()
    }
}

/// A Reader for WriteLog files. (more information on WriteLog files is to
/// be found above at WriteLogWriter).
pub struct WriteLogReader {
    src: Box<Read>,
    records_read: u32,
    bytes_read: usize,
}

impl WriteLogReader {
    pub fn new(src: Box<Read>) -> WriteLogReader {
        WriteLogReader {
            src: src,
            records_read: 0,
            bytes_read: 0,
        }
    }

    pub fn new_from_file(file: &String) -> io::Result<WriteLogReader> {
        fs::OpenOptions::new()
            .read(true)
            .open(file)
            .map(move |f| WriteLogReader::new(Box::new(f)))
    }

    pub fn get_stats(&self) -> (u32, usize) {
        (self.records_read, self.bytes_read)
    }

    // Inlining saves us up to 400ns per record (1600ns vs 2000ns)
    #[inline]
    fn read_bytes(&mut self, buf: &mut [u8], len: usize) -> io::Result<usize> {
        let mut off = 0;
        loop {
            match self.src.read(&mut buf[off..len]) {
                Err(e) => return Err(e),
                Ok(s) => {
                    if s == 0 {
                        return Err(io::Error::new(io::ErrorKind::InvalidData,
                                                  "Could not read enough data"));
                    } else if off + s < len {
                        off += s;
                    } else {
                        self.bytes_read += s;
                        return Ok(off + s);
                    }
                }
            }
        }
    }

    /// Reads as many bytes as necessary into a vector and returns it.
    /// This can of course take up much memory.
    pub fn read_vec(&mut self) -> io::Result<vec::Vec<u8>> {
        let mut lengthbuf = [0; 4];

        let mut res = self.read_bytes(&mut lengthbuf, 4);

        match res {
            Err(e) => return Err(e),
            Ok(_) => (),
        }

        let length = decode_u32(lengthbuf) as usize;
        let mut buffer = vec::Vec::with_capacity(length);
        buffer.resize(length, 0);

        res = self.read_bytes(&mut buffer[..], length);

        match res {
            Err(e) => Err(e),
            Ok(_) => {
                self.records_read += 1;
                Ok(buffer)
            }
        }
    }
}

impl Iterator for WriteLogReader {
    type Item = String;
    fn next(&mut self) -> Option<String> {
        let result = self.read_vec();
        let convert_result;

        match result {
            Err(_) => return None,
            Ok(v) => convert_result = string::String::from_utf8(v)
        }

        match convert_result {
            Err(_) => None,
            Ok(s) => Some(s)
        }
    }
}

// Byte string implementation.
/*
impl Iterator for WriteLogReader {
    type Item = vec::Vec<u8>;
    fn next(&mut self) -> Option<vec::Vec<u8>> {
        let result = self.read_vec();
        match result {
            Err(_) => None,
            Ok(v) => Some(v)
        }
    }
}
*/

impl Read for WriteLogReader {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        let mut lengthbuf = [0; 4];

        let mut res = self.read_bytes(&mut lengthbuf, 4);

        match res {
            Err(_) => return res,
            Ok(_) => (),
        }

        let mut length = decode_u32(lengthbuf) as usize;

        if dst.len() < length {
            length = dst.len();
        }

        res = self.read_bytes(dst, length);

        match res {
            Err(_) => res,
            Ok(_) => {
                self.records_read += 1;
                res
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::{encode_u32, decode_u32};
    use super::{WriteLogWriter, WriteLogReader};
    use std::vec;
    use std::io::{Read, Write};
    use std::fs;
    use std::string;

    #[test]
    fn test_u32_encoder() {
        let testvals = [0, 1, 2, 31, 199, 100000, 111111, 3000000, 4100000000];

        for val in testvals.into_iter() {
            assert_eq!(decode_u32(encode_u32(*val)), *val);
        }
    }

    #[test]
    fn test_write() {
        let buf1: vec::Vec<u8> = "abc".bytes().collect();
        let buf2: vec::Vec<u8> = "def".bytes().collect();
        let dst = vec::Vec::new();
        let mut w = WriteLogWriter::new(Box::new(dst));

        let _ = w.write(&buf1);
        let _ = w.write(&buf2);

        let (bytes, _) = w.get_stats();
        assert_eq!(bytes, 2 * (4 + 3));
    }

    #[test]
    fn test_write_read() {
        let filename = "writelog_test.wlg";
        {
            match fs::OpenOptions::new().write(true).create(true).open(filename) {
                Err(e) => panic!("{}", e),
                Ok(f) => {
                    let mut w = WriteLogWriter::new(Box::new(f));
                    let buf1: vec::Vec<u8> = "abc".bytes().collect();
                    let buf2: vec::Vec<u8> = "def".bytes().collect();

                    let _ = w.write(&buf1);
                    let _ = w.write(&buf2);

                    let (bytes, _) = w.get_stats();
                    assert_eq!(bytes, 2 * (4 + 3));
                }
            }
        }
        {
            match fs::OpenOptions::new().read(true).open(filename) {
                Err(e) => panic!("{}", e),
                Ok(f) => {
                    let mut r = WriteLogReader::new(Box::new(f));
                    let mut buf = [0; 16];

                    let res = r.read(&mut buf);
                    match res {
                        Err(e) => panic!("{}", e),
                        Ok(_) => assert_eq!(string::String::from_utf8_lossy(&buf[0..3]), "abc"),
                    }

                    let res2 = r.read(&mut buf);
                    match res2 {
                        Err(e) => panic!("{}", e),
                        Ok(_) => assert_eq!(string::String::from_utf8_lossy(&buf[0..3]), "def"),
                    }
                }
            }
        }
    }

    extern crate time;
    use self::time::PreciseTime;

    const N_ENTRIES: u32 = 1000000;

    fn bench_a_writing() {
        let buf: vec::Vec<u8> = "aaabbbcccdddeeefffggghhhiiijjjkkklllmmmnnnoooppp"
                                    .bytes()
                                    .collect();

        match WriteLogWriter::new_to_file(&String::from("bench_file.wlg"), false) {
            Err(e) => panic!("{}", e),
            Ok(ref mut writer) => {
                let start = PreciseTime::now();
                let mut j = 0;

                for _ in 0..N_ENTRIES {
                    let _ = writer.write(&buf);
                    j += 1;
                }
                let end = PreciseTime::now();
                println!("Took {} total; {} per record.",
                         start.to(end),
                         start.to(end) / N_ENTRIES as i32);
                assert_eq!(j, N_ENTRIES);

                let (bytes, _) = writer.get_stats();
                assert_eq!(bytes, (N_ENTRIES * 3 * 16 + N_ENTRIES * 4) as u64);
            }
        }
    }

    #[test]
    #[allow(unreachable_code)]
    fn bench_b_reading() {
        //! Uses the data written by bench_a_writing().
        return;
        bench_a_writing();

        match WriteLogReader::new_from_file(&String::from("bench_file.wlg")) {
            Err(e) => panic!("{}", e),
            Ok(ref mut reader) => {
                let mut buf: [u8; 16 * 4] = [0; 16 * 4];
                let mut i = 0;

                let start = PreciseTime::now();
                loop {
                    match reader.read(&mut buf) {
                        Err(e) => {
                            println!("{}", e);
                            break;
                        }
                        Ok(len) => {
                            i += 1;
                            assert_eq!(len, 16 * 3);
                        }
                    }
                }
                let end = PreciseTime::now();
                println!("Took {} total; {} per record.",
                         start.to(end),
                         start.to(end) / N_ENTRIES as i32);
                assert_eq!(i, N_ENTRIES);
                assert_eq!(reader.get_stats(),
                           (N_ENTRIES, (N_ENTRIES * 4 + N_ENTRIES * 3 * 16) as usize));
            }
        }

    }
}
