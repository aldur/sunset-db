use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{read_dir, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::os::unix::prelude::OsStrExt;
use std::path::{Path, PathBuf};

pub type Index = HashMap<String, u64>;

const SEGMENT_EXT: &str = "segment";

// TODO: Switch to using an empty byte string as the tombstone?
const TOMBSTONE: u64 = 1u64 << 63;
const ENCODED_TOMBSTONE: [u8; size_of::<u64>()] = (TOMBSTONE).to_be_bytes();

// NOTE: This will hold the file open as long as `Segment` is in memory.
struct Segment {
    id: u64,
    file: File,
    index: Index,
}

use thiserror::Error;

#[derive(Error, Debug)]
pub enum SunsetDBError {
    #[error("key not found")]
    KeyNotFound,

    #[error("there should be at least a segment")]
    NoSegments,

    // TODO: Add how much
    #[error("exceeds max size (expected < {})", u64::MAX)]
    ExceedsMaxSize,

    #[error("invalid checksum (expected {expected:?}, found {found:?})")]
    InvalidChecksum { expected: u32, found: u32 },

    #[error("io error")]
    IO(#[from] io::Error),

    // TODO: Return ID?
    #[error("invalid ID")]
    InvalidID(#[from] std::num::ParseIntError),

    #[error("invalid string")]
    InvalidString {
        #[from]
        source: std::string::FromUtf8Error,
    },

    #[error("invalid int")]
    InvalidInt(#[from] std::num::TryFromIntError),

    #[error("unexpected: {0:?}")]
    Unexpected(String),
}

pub type Result<T> = std::result::Result<T, SunsetDBError>;

impl Segment {
    pub fn new(path: &Path) -> Result<Segment> {
        let mut f = OpenOptions::new()
            .create(true) // TODO: Should not try to create all segments.
            .read(true)
            .write(true) // TODO: Only most recent segment should be open for write.
            .open(path)?;
        let index = Segment::index_from_disk(&mut f);
        Ok::<_, _>(Segment {
            id: Segment::id_from_path(path)?,
            file: f,
            index: index?,
        })
    }

    pub fn insert(&mut self, key: &str, value: &str) -> Result<()> {
        // FIXME: This check doesn't make any sense.
        if value.len() as u64 & TOMBSTONE > 0 || key.len() as u64 > u64::MAX {
            return Err(SunsetDBError::ExceedsMaxSize);
        }

        let offset = self.file.metadata()?.len();

        // TODO: Write the CRC only once per record.
        // Writing the `key` allows us to reconstruct `index` later on
        append_string(&mut self.file, key)?;
        append_string(&mut self.file, value)?;

        // TODO: no need for `to_owned` if key already there?
        // https://doc.rust-lang.org/std/collections/hash_map/enum.Entry.html
        self.index.insert(key.to_owned(), offset);

        Ok(())
    }

    pub fn delete(&mut self, key: &str) -> Result<()> {
        append_string(&mut self.file, key)?;
        append_deletion(&mut self.file)?;
        self.index.remove(key).ok_or(SunsetDBError::KeyNotFound)?;
        Ok(())
    }

    pub fn get(&mut self, key: &str) -> Result<String> {
        let mut offset: u64 = *self.index.get(key).ok_or(SunsetDBError::KeyNotFound)?;
        assert_eq!(
            read_string_at_offset(&mut self.file, offset)?
                .ok_or(SunsetDBError::Unexpected("key should be there".to_string()))?,
            key
        );

        offset += LEN_PREFIX_SIZE as u64 + key.len() as u64 + CRC32_SUFFIX_SIZE as u64;
        let value = read_string_at_offset(&mut self.file, offset)?;

        value.ok_or(SunsetDBError::KeyNotFound)
    }

    fn id_from_path(path: &Path) -> Result<u64> {
        Ok(path
            .file_stem()
            .ok_or(SunsetDBError::Unexpected("Can't get file stem".to_string()))?
            .to_str()
            .ok_or(SunsetDBError::Unexpected(
                "Can't convert file stem to &str".to_string(),
            ))?
            .parse()?)
    }

    fn index_from_disk(file: &mut File) -> Result<Index> {
        let mut index = Index::new();
        file.rewind()?; // Should not be required.

        // TODO: If possible, instead of a full disk read read from a dump of the HashMap

        let db_len = file.metadata()?.len();
        loop {
            let offset = file.stream_position()?;
            if offset == db_len {
                break;
            }

            let k = read_string_crc32(file)?
                .ok_or(SunsetDBError::Unexpected("Expected key".to_string()))?;

            // TODO: Ignore data invalid checksums.

            let len_v_b = read_u64_bytes(file)?;
            if len_v_b != ENCODED_TOMBSTONE {
                index.insert(k, offset);
                let len_v = parse_u64_bytes(len_v_b)?;
                file.seek(SeekFrom::Current(i64::try_from(
                    len_v + CRC32_SUFFIX_SIZE as u64,
                )?))?;
            } else {
                index.remove(&k);
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
    pub fn new(base_path: &Path) -> Result<SunsetDB> {
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
            .collect::<Result<Vec<_>>>()?;

        let next_index: u64;
        if let Some(s) = segments.last() {
            next_index = s.id + 1;
        } else {
            next_index = 0;
        }

        let mut db = SunsetDB {
            base_path: base_path.to_path_buf(),
            segments,
            next_index,
        };

        if db.segments.is_empty() {
            db.add_new_segment()?;
        }

        Ok(db)
    }

    fn path_from_id(&self, id: u64) -> PathBuf {
        self.base_path.join(format!("{}.{}", id, SEGMENT_EXT))
    }

    fn add_new_segment(&mut self) -> Result<()> {
        let path = self.path_from_id(self.next_index);
        self.segments.push(Segment::new(path.as_path())?);
        self.next_index += 1;
        Ok(())
    }

    pub fn insert(&mut self, key: &str, value: &str) -> Result<()> {
        let segment = self.segments.get_mut(0).ok_or(SunsetDBError::NoSegments)?; // Created in `::new`
        segment.insert(key, value)?;

        // TODO: Close segment if it grows too large.
        // TODO: Merge segments and claim space.

        Ok(())
    }

    pub fn get(&mut self, key: &str) -> Result<String> {
        for s in self.segments.iter_mut().rev() {
            if let Ok(value) = s.get(key) {
                return Ok(value);
            }
        }

        Err(SunsetDBError::KeyNotFound)
    }

    pub fn delete(&mut self, key: &str) -> Result<()> {
        let segment = self.segments.get_mut(0).ok_or(SunsetDBError::NoSegments)?; // Created in `::new`
        segment.delete(key)?;
        Ok(())
    }
}

const LEN_PREFIX_SIZE: usize = size_of::<u64>();
const CRC32_SUFFIX_SIZE: usize = size_of::<u32>();

fn append_deletion(file: &mut File) -> Result<()> {
    file.seek(io::SeekFrom::End(0))?;
    file.write_all(&ENCODED_TOMBSTONE)?;

    // XXX: Write checkum for TOMBSTONE too?
    // let checksum = crc32fast::hash(&ENCODED_TOMBSTONE);
    // write_whole(file, &checksum.to_be_bytes())?;

    Ok(())
}

fn append_string(file: &mut File, b: &str) -> Result<()> {
    file.seek(io::SeekFrom::End(0))?;

    // Cast all to u64 and use big endian to make this portable across machines.
    let b_len_bytes = b.len().to_be_bytes();
    file.write_all(&b_len_bytes)?;

    let b_bytes = b.as_bytes();
    file.write_all(b_bytes)?;

    let checksum = crc32fast::hash(b_bytes);
    file.write_all(&checksum.to_be_bytes())?;

    Ok(())
}

fn read_u64_bytes(file: &mut File) -> Result<[u8; LEN_PREFIX_SIZE]> {
    let mut int_b = [0; LEN_PREFIX_SIZE];
    file.read_exact(&mut int_b)?;
    Ok(int_b)
}

fn parse_u64_bytes(i: [u8; LEN_PREFIX_SIZE]) -> Result<u64> {
    Ok(u64::from_be_bytes(i))
}

fn read_string_crc32(file: &mut File) -> Result<Option<String>> {
    let string_len_b = read_u64_bytes(file)?;
    if string_len_b == ENCODED_TOMBSTONE {
        return Ok(None); // Deleted
    }

    let string_len = parse_u64_bytes(string_len_b)?;
    let mut string_b = vec![0; usize::try_from(string_len)?];
    file.read_exact(&mut string_b)?;

    let mut checksum_b = [0; CRC32_SUFFIX_SIZE];
    file.read_exact(&mut checksum_b)?;
    let checksum = u32::from_be_bytes(checksum_b);
    let expected = crc32fast::hash(&string_b);

    if checksum != expected {
        return Err(SunsetDBError::InvalidChecksum {
            expected,
            found: checksum,
        });
    }

    Ok(Some(String::from_utf8(string_b)?))
}

fn read_string_at_offset(file: &mut File, offset: u64) -> Result<Option<String>> {
    // TODO: Maybe use `seek_read`?
    file.seek(io::SeekFrom::Start(offset))?;
    read_string_crc32(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::{tempdir, TempDir};

    fn encoded_len(k: &str, v: &str) -> u64 {
        (LEN_PREFIX_SIZE
            + k.len()
            + CRC32_SUFFIX_SIZE
            + LEN_PREFIX_SIZE
            + v.len()
            + CRC32_SUFFIX_SIZE) as u64
    }

    fn new_base() -> io::Result<TempDir> {
        tempdir()
    }

    #[test]
    fn test_base_is_automatically_deleted() -> Result<()> {
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
    fn test_sunsetdb_empty_base_path() -> Result<()> {
        let base_dir = new_base()?;
        let s = SunsetDB::new(base_dir.path())?;
        assert_eq!(s.base_path, base_dir.path());
        assert_eq!(s.segments.len(), 1); // ::new creates a new segment by default
        Ok(())
    }

    #[test]
    fn test_sunsetdb_insert_get_delete() -> Result<()> {
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
    fn test_segment_e2e() -> Result<()> {
        let new_base = new_base()?;

        let id: u64 = 42;
        let segment_path = new_base.path().join(format!("{}.{}", id, SEGMENT_EXT));
        let mut segment = Segment::new(segment_path.as_path())?;
        assert_eq!(id, segment.id);

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
}
