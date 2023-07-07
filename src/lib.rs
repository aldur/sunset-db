pub mod sunset_db {
    use std::collections::HashMap;
    use std::error;
    use std::fs::File;
    use std::io::{self, Error, Read, Seek, SeekFrom, Write};
    use std::mem::{size_of, size_of_val};

    pub type Index = HashMap<String, u64>;

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

    pub fn put(index: &mut Index, db: &mut File, key: &str, value: &str) -> Result<(), Error> {
        let offset = db.metadata()?.len();

        // Writing the `key` allows us to reconstruct `index` later on
        append_string(db, key)?;
        append_string(db, value)?;

        index.insert(key.to_owned(), offset);

        Ok(())
    }

    pub fn get(
        index: &mut Index,
        db: &mut File,
        key: &str,
    ) -> Result<String, Box<dyn error::Error>> {
        let key_offset: &u64 = index.get(key).ok_or("Couldn't find key in index")?;
        assert_eq!(read_string_at_offset(db, key_offset)?, key);

        let value_offset = *key_offset + LEN_PREFIX_SIZE as u64 + key.len() as u64;
        let value = read_string_at_offset(db, &value_offset)?;
        Ok(value)
    }

    pub fn init_index(db: &mut File) -> Result<Index, Box<dyn error::Error>> {
        let mut index = Index::new();
        db.rewind()?;

        let db_len = db.metadata()?.len();
        loop {
            let offset = db.stream_position()?;
            if offset == db_len {
                break;
            }

            let k = read_string(db)?;

            let len_v = read_int(db)?;
            db.seek(SeekFrom::Current(i64::try_from(len_v)?))?;

            index.insert(k, offset);
        }

        Ok(index)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use tempfile::tempfile;

        fn on_disk_len(k: &str, v: &str) -> u64 {
            (LEN_PREFIX_SIZE + k.len() + LEN_PREFIX_SIZE + v.len()) as u64
        }

        fn setup_db() -> Result<File, io::Error> {
            tempfile()
        }

        #[test]
        fn test_init_not_existing() -> Result<(), Box<dyn error::Error>> {
            let mut db = setup_db()?;
            let index_from_disk = init_index(&mut db)?;
            assert_eq!(index_from_disk.len(), 0);

            Ok(())
        }

        #[test]
        fn test_e2e() -> Result<(), Box<dyn error::Error>> {
            let mut db = setup_db()?;
            let mut index = init_index(&mut db)?;

            let inputs = [
                ("foo", "bar"),
                ("biz", "boo"),
                ("long_one", "continuing"),
                ("biz", "boo2"),
            ];

            for (k, v) in inputs {
                let f_size = db.metadata()?.len();
                put(&mut index, &mut db, k, v)?;
                let delta = db.metadata()?.len() - f_size;
                assert_eq!(delta, on_disk_len(k, v));

                let vv = get(&mut index, &mut db, k)?;
                assert_eq!(vv, v);
            }

            let vv = get(&mut index, &mut db, "biz")?;
            assert_eq!(vv, "boo2");

            let inputs_sum: u64 = inputs.iter().map(|(k, v)| on_disk_len(k, v)).sum();
            assert_eq!(db.metadata()?.len(), inputs_sum);

            let index_from_disk = init_index(&mut db)?;
            assert_eq!(index_from_disk, index);

            Ok(())
        }
    }
}
