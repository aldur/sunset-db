pub mod sunset_db {
    use std::collections::HashMap;
    use std::error;
    use std::ffi::OsStr;
    use std::fmt;
    use std::fs::{read_dir, File, OpenOptions};
    use std::io::{self, Read, Seek, SeekFrom, Write};
    use std::mem::{size_of, size_of_val};
    use std::os::unix::prelude::OsStrExt;
    use std::path::{Path, PathBuf};

    pub type Index = HashMap<String, u64>;
    const SEGMENT_EXT: &str = "segment";

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
            let offset = self.file.metadata()?.len();

            // Writing the `key` allows us to reconstruct `index` later on
            append_string(&mut self.file, key)?;
            append_string(&mut self.file, value)?;

            self.index.insert(key.to_owned(), offset);

            Ok(())
        }

        pub fn get(&mut self, key: &str) -> Result<String, Box<dyn error::Error>> {
            let key_offset: &u64 = self.index.get(key).ok_or(KeyNotFoundError)?;
            assert_eq!(read_string_at_offset(&mut self.file, key_offset)?, key);

            let value_offset = *key_offset + LEN_PREFIX_SIZE as u64 + key.len() as u64;
            let value = read_string_at_offset(&mut self.file, &value_offset)?;
            Ok(value)
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

                let k = read_string(file)?;

                let len_v = read_int(file)?;
                file.seek(SeekFrom::Current(i64::try_from(len_v)?))?;

                index.insert(k, offset);
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
            // TODO: Implement me!
            for s in self.segments.iter_mut().rev() {
                if let Ok(value) = s.get(key) {
                    return Ok(value);
                }
            }

            Err(KeyNotFoundError)
        }

        pub fn delete(&self, _key: &str) -> Result<(), Box<dyn error::Error>> {
            // TODO: Implement me!
            Ok(())
        }
    }

    const LEN_PREFIX_SIZE: usize = size_of::<u64>();

    fn append_string(file: &mut File, b: &str) -> Result<(), io::Error> {
        file.seek(io::SeekFrom::End(0))?;

        // Cast all to u64 and use big endian to make this portable across machines.
        let b_len = b.len() as u64;
        let mut remaining = size_of_val(&b_len) as u64;
        let b_len_bytes = &b_len.to_be_bytes();

        while remaining > 0 {
            match file.write(b_len_bytes) {
                Err(e) => return Err(e),
                Ok(written) => remaining -= written as u64,
            }
        }

        remaining = b_len;

        while remaining > 0 {
            match file.write(b.as_bytes()) {
                Err(e) => return Err(e),
                Ok(written) => remaining -= written as u64,
            }
        }

        // TODO: Add checksum!

        Ok(())
    }

    fn read_int(file: &mut File) -> Result<u64, Box<dyn error::Error>> {
        let mut int_b = [0; LEN_PREFIX_SIZE];
        file.read_exact(&mut int_b)?;
        Ok(u64::from_be_bytes(int_b))
    }

    fn read_string(file: &mut File) -> Result<String, Box<dyn error::Error>> {
        let string_len = read_int(file)?;

        let mut string_b = vec![0; usize::try_from(string_len)?];
        file.read_exact(&mut string_b)?;

        Ok(String::from_utf8(string_b)?)
    }

    fn read_string_at_offset(
        file: &mut File,
        offset: &u64,
    ) -> Result<String, Box<dyn error::Error>> {
        // TODO: Maybe use `seek_read`?
        file.seek(io::SeekFrom::Start(*offset))?;
        read_string(file)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use tempfile::{tempdir, TempDir};

        fn encoded_len(k: &str, v: &str) -> u64 {
            (LEN_PREFIX_SIZE + k.len() + LEN_PREFIX_SIZE + v.len()) as u64
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
        fn test_sunsetdb_insert_get() -> Result<(), Box<dyn error::Error>> {
            let base_dir = new_base()?;
            let mut s = SunsetDB::new(base_dir.path())?;

            s.insert("k", "v")?;
            assert_eq!(s.get("k")?, "v");

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

            let segment_from_disk = Segment::new(segment_path.as_path())?;
            assert_eq!(segment_from_disk.index, segment.index);

            Ok(())
        }
    }
}
