use std::io::Error;
use std::io::Result;
use std::path::Path;

use sled::Db;

use super::KvsEngine;

pub struct SledKvStore {
    db: Db,
}

impl SledKvStore {
   pub fn open<P: AsRef<Path>>(path: P) -> Result<SledKvStore> {
        let db = sled::open(path)?;
        Ok(SledKvStore { db })
    }
}

impl KvsEngine for SledKvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key, value.into_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        self.db.get(key)?
            .map(|ivec| String::from_utf8(ivec.to_vec()))
            .transpose()
            .map_err(|e| Error::other(e.to_string()))
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.db.remove(key)?;
        self.db.flush()?;
        Ok(())
    }
}
