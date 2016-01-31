//! Module that uses text files as input to the mapper phase.
//! This module implements only an iterator yielding single lines;
//! using the RecordIterator from formats::util, the necessary key/value
//! iterator can be implemented.

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

#[cfg(test)]
mod test {
    use formats::lines;

    #[test]
    fn test_read_file() {
        let file = "Cargo.toml";
        let it;
        match lines::new_from_file(&String::from(file)) {
            Err(e) => panic!("{}", e),
            Ok(r) => it = r,
        }

        for line in it {
            println!("{}", line);
        }
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

        println!("Reading...");
        for line in it {
            println!("{}", line);
        }
    }
}
