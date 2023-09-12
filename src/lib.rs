mod error;

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{read_dir, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::os::unix::prelude::OsStrExt;
use std::path::{Path, PathBuf};
use std::result::Result;

use self::error::*;

type Index = HashMap<String, u64>;

const SEGMENT_EXT: &str = "segment";

// TODO: Switch to using an empty byte string as the tombstone?
const TOMBSTONE: u64 = 1u64 << 63;
const ENCODED_TOMBSTONE: [u8; size_of::<u64>()] = (TOMBSTONE).to_be_bytes();

#[derive(Debug)]
struct SegmentID(u64);

impl std::str::FromStr for SegmentID {
    type Err = SegmentIDError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(SegmentID(
            u64::from_str(s).map_err(|_| SegmentIDError::NotAnInt)?,
        ))
    }
}

impl TryFrom<&Path> for SegmentID {
    type Error = SegmentIDError;

    fn try_from(path: &Path) -> Result<Self, Self::Error> {
        path.file_stem()
            .ok_or(SegmentIDError::IDFromEmtpyPath)?
            .to_str()
            .ok_or(SegmentIDError::IDFromInvalidPath(path.to_path_buf()))?
            .parse()
    }
}

// NOTE: This will hold the file open as long as `Segment` is in memory.
struct Segment {
    id: SegmentID,
    file: File,
    index: Index,
}

impl Segment {
    fn new(path: &Path) -> Result<Segment, SegmentError> {
        let mut f = OpenOptions::new()
            .create(true) // TODO: Should not try to create all segments.
            .read(true)
            .write(true) // TODO: Only most recent segment should be open for write.
            .open(path)
            .map_err(|e| SegmentError::IOErrorAtPath {
                path: path.to_path_buf(),
                source: e,
            })?;
        let index = Segment::index_from_disk(&mut f);
        Ok::<_, _>(Segment {
            id: SegmentID::try_from(path)
                .map_err(|_| SegmentError::InvalidPath(path.to_path_buf()))?,
            file: f,
            index: index?,
        })
    }

    fn insert(&mut self, key: &str, value: &str) -> Result<(), InsertError> {
        // `append_string` encodes the `len`, then the string.
        // `append_deletion` stores `TOMBSTONE` after the key.
        // Having a `value` with a `len` equal to the TOMBSTONE would
        // allow confusing it with a deleted entry.
        // Could be a strict `==`, we make it >= so that there's a clear max size.
        if value.len() as u64 >= TOMBSTONE {
            return Err(InsertError::ValueExceedsMaxSize);
        }

        if key.len() as u128 > (u64::MAX as u128) {
            return Err(InsertError::KeyExceedsMaxSize);
        }

        let offset = self.file.metadata()?.len();

        // NOTE: We could write the CRC only once per record.
        // NOTE: Writing the `key` isn't strictly required,
        // but it allows us to reconstruct `index` later on.
        append_string(&mut self.file, key)?;
        append_string(&mut self.file, value)?;

        // TODO: no need for `to_owned` if key already there?
        // https://doc.rust-lang.org/std/collections/hash_map/enum.Entry.html
        self.index.insert(key.to_owned(), offset);

        Ok(())
    }

    fn delete(&mut self, key: &str) -> Result<(), DeleteError> {
        append_string(&mut self.file, key)?;
        append_deletion(&mut self.file)?;
        self.index.remove(key).ok_or(DeleteError::KeyNotFound)?;
        Ok(())
    }

    fn get(&mut self, key: &str) -> Result<String, GetError> {
        let mut offset: u64 = *self.index.get(key).ok_or(GetError::KeyNotFound)?;
        debug_assert!(
            read_string_at_offset(&mut self.file, offset)
                .is_ok_and(|v| v.is_some_and(|s| s == key)),
            "should find key at offset from index"
        );

        offset += ENCODED_LEN_SIZE as u64 + key.len() as u64 + CRC32_SIZE as u64;
        let value = read_string_at_offset(&mut self.file, offset)?;

        value.ok_or(GetError::KeyNotFound)
    }

    fn index_from_disk(file: &mut File) -> Result<Index, SegmentError> {
        let mut index = Index::new();
        file.rewind()?; // Should not be required.

        // TODO: If possible, instead of a full disk read from a dump of the HashMap

        let segment_len = file.metadata()?.len();
        loop {
            let offset = file.stream_position()?;
            if offset == segment_len {
                break;
            }

            let key = read_check_string(file)?.ok_or(SegmentError::InvalidIndexFormat(
                "tombstone in index".to_string(),
            ))?;

            // TODO: Ignore keys for values having an invalid checksum.

            let encoded_value_len = read_u64_bytes(file)?;
            if encoded_value_len != ENCODED_TOMBSTONE {
                index.insert(key, offset);
                let value_len = parse_u64_bytes(encoded_value_len)?;
                let end_of_encoded_entry = i64::try_from(value_len + CRC32_SIZE as u64)
                    .map_err(|_| SegmentError::SeekError)?;
                file.seek(SeekFrom::Current(end_of_encoded_entry))?;
            } else {
                index.remove(&key);
            }
        }

        Ok(index)
    }
}

pub struct SunsetDB {
    base_path: PathBuf,
    segments: Vec<Segment>,
    next_index: u64,
}

impl SunsetDB {
    pub fn new(base_path: &Path) -> Result<SunsetDB, SunsetDBError> {
        let mut paths: Vec<_> = read_dir(base_path)?
            // WARNING: This will filter out errors on `read_dir`.
            .filter_map(std::io::Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension() == Some(OsStr::from_bytes(SEGMENT_EXT.as_bytes())))
            .collect();

        // least to most recent ID
        paths.sort(); // read_dir does not guarantee sorting

        let segments = paths
            .iter()
            .map(|p| Segment::new(p))
            .collect::<Result<Vec<_>, _>>()?;

        let next_index: u64;
        if let Some(s) = segments.last() {
            next_index = s.id.0 + 1;
        } else {
            next_index = 0;
        }

        let mut sunset = SunsetDB {
            base_path: base_path.to_path_buf(),
            segments,
            next_index,
        };

        if sunset.segments.is_empty() {
            sunset.add_new_segment()?;
        }

        Ok(sunset)
    }

    fn path_from_id(&self, id: u64) -> PathBuf {
        self.base_path.join(format!("{}.{}", id, SEGMENT_EXT))
    }

    fn add_new_segment(&mut self) -> Result<(), SunsetDBError> {
        // TODO: We take the index, make it a path, then the segment needs to
        // re-parse it to know its own index. Strange.
        let path = self.path_from_id(self.next_index);
        self.segments.push(Segment::new(path.as_path())?);
        self.next_index += 1;
        Ok(())
    }

    pub fn insert(&mut self, key: &str, value: &str) -> Result<(), InsertError> {
        let segment = self.segments.get_mut(0).ok_or(InsertError::NoSegments)?; // Created in `::new`
        segment.insert(key, value)?;

        // TODO: Close segment if it grows too large.
        // TODO: Merge segments and claim space.

        Ok(())
    }

    pub fn get(&mut self, key: &str) -> Result<String, GetError> {
        for s in self.segments.iter_mut().rev() {
            if let Ok(value) = s.get(key) {
                return Ok(value);
            }
        }

        Err(GetError::KeyNotFound)
    }

    pub fn delete(&mut self, key: &str) -> Result<(), DeleteError> {
        let segment = self.segments.get_mut(0).ok_or(DeleteError::NoSegments)?; // Created in `::new`
        segment.delete(key)?;
        Ok(())
    }
}

const ENCODED_LEN_SIZE: usize = size_of::<u64>();
const CRC32_SIZE: usize = size_of::<u32>();

// -- <TOMBSTONE> --
fn append_deletion(file: &mut File) -> Result<(), io::Error> {
    file.seek(io::SeekFrom::End(0))?;
    file.write_all(&ENCODED_TOMBSTONE)?;

    // XXX: Write checkum for TOMBSTONE too?
    // let checksum = crc32fast::hash(&ENCODED_TOMBSTONE);
    // write_whole(file, &checksum.to_be_bytes())?;

    Ok(())
}

// -- <len> || <string> || <checksum> --
fn append_string(file: &mut File, b: &str) -> Result<(), io::Error> {
    file.seek(io::SeekFrom::End(0))?;

    // Cast all to u64 and use big endian to make this portable across machines.
    let encoded_len = b.len().to_be_bytes();
    file.write_all(&encoded_len)?;

    let encoded_b = b.as_bytes();
    file.write_all(encoded_b)?;

    let checksum = crc32fast::hash(encoded_b);
    file.write_all(&checksum.to_be_bytes())?;

    Ok(())
}

fn read_u64_bytes(file: &mut File) -> Result<[u8; ENCODED_LEN_SIZE], ReadError> {
    let mut read_buffer = [0; ENCODED_LEN_SIZE];
    file.read_exact(&mut read_buffer)?;
    Ok(read_buffer)
}

fn parse_u64_bytes(bytes: [u8; ENCODED_LEN_SIZE]) -> Result<u64, ReadError> {
    Ok(u64::from_be_bytes(bytes))
}

fn read_check_string(file: &mut File) -> Result<Option<String>, ReadError> {
    // TODO: Would it be faster to read a bigger chunk into a static array?
    let encoded_string_len = read_u64_bytes(file)?;
    if encoded_string_len == ENCODED_TOMBSTONE {
        return Ok(None); // Deleted
    }

    let string_len = parse_u64_bytes(encoded_string_len)?;
    let mut encoded_string = vec![0; usize::try_from(string_len)?];
    file.read_exact(&mut encoded_string)?;

    let mut encoded_checksum = [0; CRC32_SIZE];
    file.read_exact(&mut encoded_checksum)?;
    let checksum = u32::from_be_bytes(encoded_checksum);
    let expected = crc32fast::hash(&encoded_string);

    if checksum != expected {
        return Err(ReadError::InvalidChecksum {
            expected,
            found: checksum,
        });
    }

    Ok(Some(String::from_utf8(encoded_string)?))
}

fn read_string_at_offset(file: &mut File, offset: u64) -> Result<Option<String>, ReadError> {
    // TODO: Maybe use `seek_read`?
    file.seek(io::SeekFrom::Start(offset))?;
    read_check_string(file)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;
    use tempfile::{tempdir, TempDir};

    // Bad practice, but anything's allowed in tests :)
    type TestResult = Result<(), Box<dyn Error>>;

    fn encoded_len(k: &str, v: &str) -> u64 {
        (ENCODED_LEN_SIZE + k.len() + CRC32_SIZE + ENCODED_LEN_SIZE + v.len() + CRC32_SIZE) as u64
    }

    fn new_base() -> io::Result<TempDir> {
        tempdir()
    }

    #[test]
    fn base_is_automatically_deleted_test() -> TestResult {
        let created_p: PathBuf;

        {
            let base_dir = new_base()?;
            let s = SunsetDB::new(base_dir.path())?;
            created_p = s.base_path;
            assert!(created_p.exists()); // move
        }

        assert!(!created_p.exists());
        Ok(())
    }

    #[test]
    fn sunsetdb_empty_base_path_test() -> TestResult {
        let base_dir = new_base()?;
        let s = SunsetDB::new(base_dir.path())?;
        assert_eq!(s.base_path, base_dir.path());
        assert_eq!(s.segments.len(), 1); // ::new creates a new segment by default
        Ok(())
    }

    #[test]
    fn sunsetdb_insert_get_delete_test() -> TestResult {
        let base_dir = new_base()?;
        let mut s = SunsetDB::new(base_dir.path())?;

        s.insert("k", "v")?;
        assert_eq!(s.get("k")?, "v");
        s.insert("k", "vv")?;
        assert_eq!(s.get("k")?, "vv");
        s.delete("k")?;
        assert!(s.delete("k").is_err());

        Ok(())
    }

    #[test]
    fn segment_e2e_test() -> TestResult {
        let new_base = new_base()?;

        let id: u64 = 42;
        let segment_path = new_base.path().join(format!("{}.{}", id, SEGMENT_EXT));
        let mut segment = Segment::new(segment_path.as_path())?;
        assert_eq!(id, segment.id.0);

        let inputs = [
            ("foo", "bar"),
            ("biz", "boo"),
            ("long_one", "continuing"),
            ("biz", "boo2"),
            ("", ""),
            ("", "x"),
        ];

        for (k, v) in inputs {
            let f_size = segment_path.metadata()?.len();
            segment.insert(k, v)?;
            let delta = segment_path.metadata()?.len() - f_size;
            assert_eq!(delta, encoded_len(k, v));

            let vv = segment.get(k)?;
            assert_eq!(vv, v);
        }

        let vv = segment.get("biz")?;
        assert_eq!(vv, "boo2");

        let inputs_sum: u64 = inputs.iter().map(|(k, v)| encoded_len(k, v)).sum();
        assert_eq!(segment_path.metadata()?.len(), inputs_sum);

        segment.delete("biz")?;

        let segment_from_disk = Segment::new(segment_path.as_path())?;
        assert_eq!(segment_from_disk.index, segment.index);

        Ok(())
    }

    #[test]
    fn segment_id_test() -> TestResult {
        let id: u64 = 42;
        let binding = new_base()?.path().join(format!("{}.{}", id, SEGMENT_EXT));
        let segment_path = binding.as_path();
        let _segment_id = SegmentID::try_from(segment_path)?;
        assert!(_segment_id.0 == id);

        let empty_path = PathBuf::new();
        assert!(SegmentID::try_from(empty_path.as_path())
            .is_err_and(|e| e == SegmentIDError::IDFromEmtpyPath));

        Ok(())
    }

    #[test]
    fn sunsetdb_io_error_test() -> TestResult {
        let empty_path = PathBuf::new();
        let maybe_db = SunsetDB::new(empty_path.as_path());

        assert!(matches!(maybe_db, Err(SunsetDBError::IOError(_))));

        Ok(())
    }

    #[test]
    fn sunsetdb_force_segment_error_test() -> TestResult {
        let base_dir = new_base()?;
        let mut s = SunsetDB::new(base_dir.path())?;

        base_dir.close()?; // This deletes the temporary directory.

        let maybe_success = s.add_new_segment();
        assert!(matches!(
            maybe_success,
            Err(SunsetDBError::SegmentError(segment_error)) if matches!(&segment_error, SegmentError::IOErrorAtPath { path: _, source: _ })
        ));

        Ok(())
    }
}
