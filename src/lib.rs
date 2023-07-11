pub mod sunset_db {
    use std::collections::HashMap;
    use std::error;
    use std::ffi::OsStr;
    use std::fmt;
    use std::fs::{read_dir, File, OpenOptions};
    use std::io::{self, Read, Seek, SeekFrom, Write};
    use std::mem::size_of;
    use std::os::unix::prelude::OsStrExt;
    use std::path::{Path, PathBuf};

    pub type Index = HashMap<String, u64>;

    const SEGMENT_EXT: &str = "segment";

    const TOMBSTONE: u64 = 1u64 << 63;
    const ENCODED_TOMBSTONE: [u8; 8] = (TOMBSTONE).to_be_bytes();

    // NOTE: This will hold the file open 'til `Segment` is in memory.
    struct Segment {
        id: u64,
        file: File,
        index: Index,
    }

    #[derive(Debug, Clone)]
    pub struct KeyNotFoundError;
    impl fmt::Display for KeyNotFoundError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "key not found")
        }
    }
    impl error::Error for KeyNotFoundError {}

    #[derive(Debug, Clone)]
    pub struct ValueTooBigError;
    impl fmt::Display for ValueTooBigError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "value too big")
        }
    }
    impl error::Error for ValueTooBigError {}

    #[derive(Debug, Clone)]
    pub struct InvalidChecksumError;
    impl fmt::Display for InvalidChecksumError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "invalid checksum")
        }
    }
    impl error::Error for InvalidChecksumError {}

    impl Segment {
        pub fn new(path: &Path) -> Result<Segment, Box<dyn error::Error>> {
            let mut f = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(path)?;
            let index = Segment::index_from_disk(&mut f);
            Ok::<_, Box<dyn error::Error>>(Segment {
                id: Segment::id_from_path(path)?,
                file: f,
                index: index?,
            })
        }

        pub fn insert(&mut self, key: &str, value: &str) -> Result<(), Box<dyn error::Error>> {
            if value.len() as u64 & TOMBSTONE > 0 {
                return Err(Box::new(ValueTooBigError));
            }

            let offset = self.file.metadata()?.len();

            // Writing the `key` allows us to reconstruct `index` later on
            append_string(&mut self.file, key)?;
            append_string(&mut self.file, value)?;

            // TODO: no need for `to_owned` if key already there?
            // https://doc.rust-lang.org/std/collections/hash_map/enum.Entry.html
            self.index.insert(key.to_owned(), offset);

            Ok(())
        }

        pub fn delete(&mut self, key: &str) -> Result<(), Box<dyn error::Error>> {
            append_string(&mut self.file, key)?;
            append_deletion(&mut self.file)?;
            self.index.remove(key).ok_or(KeyNotFoundError)?;
            Ok(())
        }

        pub fn get(&mut self, key: &str) -> Result<String, Box<dyn error::Error>> {
            let mut offset: u64 = *self.index.get(key).ok_or(KeyNotFoundError)?;
            assert_eq!(
                read_string_at_offset(&mut self.file, offset)?.ok_or("key should be there")?,
                key
            );

            offset += LEN_PREFIX_SIZE as u64 + key.len() as u64 + CRC32_SUFFIX_SIZE as u64;
            let value = read_string_at_offset(&mut self.file, offset)?;

            value.ok_or(Box::new(KeyNotFoundError))
        }

        fn id_from_path(path: &Path) -> Result<u64, Box<dyn error::Error>> {
            Ok(path
                .file_stem()
                .ok_or("Can't get file stem")?
                .to_str()
                .ok_or("Can't convert file stem to &str")?
                .parse()?)
        }

        fn index_from_disk(file: &mut File) -> Result<Index, Box<dyn error::Error>> {
            let mut index = Index::new();
            file.rewind()?; // Should not be required.

            let db_len = file.metadata()?.len();
            loop {
                let offset = file.stream_position()?;
                if offset == db_len {
                    break;
                }

                let k = read_string_crc32(file)?.ok_or("Expected key")?;

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
        pub fn new(base_path: &Path) -> Result<SunsetDB, Box<dyn error::Error>> {
            let mut paths: Vec<_> = read_dir(base_path)?
                // WARNING: This will filter out errors on `read_dir`.
                .filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| p.extension() == Some(OsStr::from_bytes(SEGMENT_EXT.as_bytes())))
                .collect();

            // least to most recent timestamp
            paths.sort(); // read_dir does not guarantee sorting

            let segments = paths
                .iter()
                .map(|p| Segment::new(p))
                .collect::<Result<Vec<_>, _>>()?;

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

        fn add_new_segment(&mut self) -> Result<(), Box<dyn error::Error>> {
            let path = self.path_from_id(self.next_index);
            self.segments.push(Segment::new(path.as_path())?);
            self.next_index += 1;
            Ok(())
        }

        pub fn insert(&mut self, key: &str, value: &str) -> Result<(), Box<dyn error::Error>> {
            let segment = self
                .segments
                .get_mut(0)
                .ok_or("there should be at least one segment")?; // Created in `::new`
            segment.insert(key, value)?;

            // TODO: Close segment if it grows too large.

            Ok(())
        }

        pub fn get(&mut self, key: &str) -> Result<String, KeyNotFoundError> {
            for s in self.segments.iter_mut().rev() {
                if let Ok(value) = s.get(key) {
                    return Ok(value);
                }
            }

            Err(KeyNotFoundError)
        }

        pub fn delete(&mut self, key: &str) -> Result<(), Box<dyn error::Error>> {
            let segment = self
                .segments
                .get_mut(0)
                .ok_or("there should be at least one segment")?; // Created in `::new`
            segment.delete(key)?;
            Ok(())
        }
    }

    const LEN_PREFIX_SIZE: usize = size_of::<u64>();
    const CRC32_SUFFIX_SIZE: usize = size_of::<u32>();

    fn append_deletion(file: &mut File) -> Result<(), Box<dyn error::Error>> {
        file.seek(io::SeekFrom::End(0))?;
        file.write_all(&ENCODED_TOMBSTONE)?;

        // XXX: Write checkum for TOMBSTONE too?
        // let checksum = crc32fast::hash(&ENCODED_TOMBSTONE);
        // write_whole(file, &checksum.to_be_bytes())?;

        Ok(())
    }

    fn append_string(file: &mut File, b: &str) -> Result<(), Box<dyn error::Error>> {
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

    fn read_u64_bytes(file: &mut File) -> Result<[u8; LEN_PREFIX_SIZE], Box<dyn error::Error>> {
        let mut int_b = [0; LEN_PREFIX_SIZE];
        file.read_exact(&mut int_b)?;
        Ok(int_b)
    }

    fn parse_u64_bytes(i: [u8; LEN_PREFIX_SIZE]) -> Result<u64, Box<dyn error::Error>> {
        Ok(u64::from_be_bytes(i))
    }

    fn read_string_crc32(file: &mut File) -> Result<Option<String>, Box<dyn error::Error>> {
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

        if checksum != crc32fast::hash(&string_b) {
            return Err(Box::new(InvalidChecksumError));
        }

        Ok(Some(String::from_utf8(string_b)?))
    }

    fn read_string_at_offset(
        file: &mut File,
        offset: u64,
    ) -> Result<Option<String>, Box<dyn error::Error>> {
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
        fn test_base_is_automatically_deleted() -> Result<(), Box<dyn error::Error>> {
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
        fn test_sunsetdb_empty_base_path() -> Result<(), Box<dyn error::Error>> {
            let base_dir = new_base()?;
            let s = SunsetDB::new(base_dir.path())?;
            assert_eq!(s.base_path, base_dir.path());
            assert_eq!(s.segments.len(), 1); // ::new creates a new segment by default
            Ok(())
        }

        #[test]
        fn test_sunsetdb_insert_get_delete() -> Result<(), Box<dyn error::Error>> {
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
        fn test_segment_e2e() -> Result<(), Box<dyn error::Error>> {
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
}
