use std::collections::HashMap;
use std::error;
use std::fs::File;
use std::io::{self, Error, Read, Seek, SeekFrom, Write};
use std::mem::{size_of, size_of_val};
use std::path::Path;

type Index = HashMap<String, u64>;

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

fn read_string_at_offset(file: &mut File, offset: &u64) -> Result<String, Box<dyn error::Error>> {
    // TODO: Maybe use `seek_read`?
    file.seek(io::SeekFrom::Start(*offset))?;

    read_string(file)
}

fn put(index: &mut Index, db: &mut File, key: &str, value: &str) -> Result<(), Error> {
    let offset = db.metadata()?.len();

    // Writing the `key` allows us to reconstruct `index` later on
    append_string(db, key)?;
    append_string(db, value)?;

    index.insert(key.to_owned(), offset);

    Ok(())
}

fn get(index: &mut Index, db: &mut File, key: &str) -> Result<String, Box<dyn error::Error>> {
    let key_offset: &u64 = index.get(key).ok_or("Couldn't find key in index")?;
    assert_eq!(read_string_at_offset(db, key_offset)?, key);

    let value_offset = *key_offset + LEN_PREFIX_SIZE as u64 + key.len() as u64;
    let value = read_string_at_offset(db, &value_offset)?;
    Ok(value)
}

fn init(db_path: &Path) -> Result<Index, Box<dyn error::Error>> {
    let mut index = Index::new();

    let mut db = File::options().read(true).open(db_path)?;
    let db_len = db.metadata()?.len();

    loop {
        let offset = db.stream_position()?;
        if offset == db_len {
            break;
        }

        let k = read_string(&mut db)?;

        let len_v = read_int(&mut db)?;
        db.seek(SeekFrom::Current(i64::try_from(len_v)?))?;

        index.insert(k, offset);
    }

    Ok(index)
}

#[cfg(test)]
mod tests {
    use std::env::temp_dir;

    use crate::*;

    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }

    fn on_disk_len(k: &str, v: &str) -> u64 {
        (LEN_PREFIX_SIZE + k.len() + LEN_PREFIX_SIZE + v.len()) as u64
    }

    #[test]
    fn test_e2e() {
        let mut index = Index::new();

        // TODO: This will clash on multiple test instances
        let db_path = temp_dir().join("sunset.db");
        {
            let mut db = File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&db_path)
                .expect("Can't open DB file");

            let inputs = [
                ("foo", "bar"),
                ("biz", "boo"),
                ("long_one", "continuing"),
                ("biz", "boo2"),
            ];

            for (k, v) in inputs {
                let f_size = db.metadata().unwrap().len();
                put(&mut index, &mut db, k, v).unwrap();
                let delta = db.metadata().unwrap().len() - f_size;
                assert_eq!(delta, on_disk_len(k, v));

                let vv = get(&mut index, &mut db, k).unwrap();
                assert_eq!(vv, v);
            }

            let vv = get(&mut index, &mut db, "biz").unwrap();
            assert_eq!(vv, "boo2");

            let inputs_sum: u64 = inputs.iter().map(|(k, v)| on_disk_len(k, v)).sum();
            assert_eq!(db.metadata().unwrap().len(), inputs_sum);
        } // Ensure DB is closed

        let index_from_disk = init(&db_path).expect("Failed to read index from disk");
        assert_eq!(index_from_disk, index);
    }
}
