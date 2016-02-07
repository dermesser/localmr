//! Module that uses text files as input to the mapper phase.
//! This module implements only an iterator yielding single lines;
//! using the RecordIterator from formats::util, the necessary key/value
//! iterator can be implemented.

use formats::util;
use std::fs;
use std::io;
use std::io::{Read, Lines, BufRead};

type LinesIterator<Src> = io::Lines<io::BufReader<Src>>;

pub struct LinesReader<Src: Read> {
    src: Box<LinesIterator<Src>>,
}

/// Returns a LinesReader reading lines from stdin.
pub fn new_from_stdin() -> LinesReader<io::Stdin> {
    LinesReader { src: Box::new(io::BufReader::new(io::stdin()).lines()) }
}

/// Returns a LinesReader reading from the given file. If you have several
/// files, you can easily use the chain() method to chain several readers.
pub fn new_from_file(path: &String) -> io::Result<LinesReader<fs::File>> {
    fs::OpenOptions::new()
        .read(true)
        .open(path)
        .map(move |f| LinesReader { src: Box::new(io::BufReader::new(f).lines()) })
}

/// Returns a LinesReader reading from all files in the given directory that have
/// a given suffix. (This needs to use dynamic dispatch internally, because otherwise
/// the type would need to represent the number of files that are used; the overhead however
/// is low compared to disk accesses).
pub fn new_from_dir(path: &String, with_suffix: &String) -> io::Result<LinesReader<Box<Read>>> {
    let mut reader: Box<Read> = Box::new(io::empty());
    let dir = try!(fs::read_dir(path));

    for entry in dir {
        let name;
        match entry {
            Err(e) => {
                println!("Could not read file from {:?}: {}", path, e);
                continue;
            }
            Ok(direntry) => name = direntry.path(),
        }

        // ugh
        if String::from(&*name.to_string_lossy()).ends_with(with_suffix) {
            match fs::OpenOptions::new().read(true).open(name.clone()) {
                Err(e) => println!("Could not open file {:?}: {}", name, e),
                Ok(f) => reader = Box::new(reader.chain(f)),
            }
        }
    }
    Ok(LinesReader { src: Box::new(io::BufReader::new(reader).lines()) })
}

/// Iterate over the lines from a LinesReader.
impl<Src: Read> Iterator for LinesReader<Src> {
    type Item = String;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.src.next() {
                None => return None,
                Some(Err(_)) => continue,
                Some(Ok(s)) => return Some(s),
            }
        }
    }
}

/// Writer that separates the chunks written by '\n' characters.
pub struct LinesWriter<W: io::Write> {
    file: W,
}

impl LinesWriter<fs::File> {
    pub fn new_to_file(path: &String) -> io::Result<LinesWriter<fs::File>> {
        let f = try!(fs::OpenOptions::new().write(true).create(true).truncate(true).open(path));
        Ok(LinesWriter { file: f })
    }
}

impl<W: io::Write> LinesWriter<W> {
    pub fn new_to_write(w: W) -> LinesWriter<W> {
        LinesWriter { file: w }
    }
}

impl<W: io::Write> io::Write for LinesWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.file.write(buf).and(self.file.write(&['\n' as u8]))
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// An MRSinkGenerator type that uses a simple path as base
/// and creates text files based on it.
#[allow(dead_code)]
pub struct LinesSinkGenerator {
    // bogus field
    i: i32,
}

impl LinesSinkGenerator {
    /// Use either a path like `/a/b/c/` to generate files in a directory
    /// or `/a/b/c/file_prefix_` to create files with that prefix.
    pub fn new_to_files() -> LinesSinkGenerator {
        LinesSinkGenerator { i: 0 }
    }
}

impl util::MRSinkGenerator for LinesSinkGenerator {
    type Sink = LinesWriter<fs::File>;
    fn new_output(&mut self, p: &String) -> Self::Sink {
        let f = fs::OpenOptions::new().write(true).truncate(true).create(true).open(p);
        match f {
            Err(e) => panic!("Couldn't open lines output file {}: {}", p, e),
            Ok(f) => return LinesWriter { file: f },
        }
    }
}

#[cfg(test)]
mod test {
    use formats::lines;
    use formats::util::MRSinkGenerator;
    use std::fs;
    use std::io::Write;

    #[test]
    fn test_read_file() {
        let file = "Cargo.toml";
        let it;
        match lines::new_from_file(&String::from(file)) {
            Err(e) => panic!("{}", e),
            Ok(r) => it = r,
        }

        let mut cnt = 0;
        for _ in it {
            cnt += 1;
        }
        assert!(cnt > 5);
    }

    #[test]
    fn test_read_dir() {
        let path = String::from("src/");
        let suffix = String::from(".rs");
        let it;
        match lines::new_from_dir(&path, &suffix) {
            Err(e) => panic!("{}", e),
            Ok(r) => it = r,
        }

        let mut cnt = 0;
        for _ in it {
            cnt += 1;
        }
        assert!(cnt > 300);
    }

    #[test]
    fn test_write_lines() {
        let line = String::from("abc def hello world");
        let mut gen = lines::LinesSinkGenerator::new_to_files();
        let mut f = gen.new_output(&String::from("testdata/writelines_1"));

        for _ in 0..10 {
            let _ = f.write(line.as_bytes());
        }

        {
            assert_eq!(fs::OpenOptions::new()
                           .read(true)
                           .open("testdata/writelines_1")
                           .unwrap()
                           .metadata()
                           .unwrap()
                           .len(),
                       200);
        }
        let _ = fs::remove_file("testdata/writelines_1");
    }
}
