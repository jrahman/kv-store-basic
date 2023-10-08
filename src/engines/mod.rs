use std::io::Result;

pub trait KvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()>;

    fn get(&mut self, key: String) -> Result<Option<String>>;

    fn remove(&mut self, key: String) -> Result<()>;
}

mod kvs;
mod sled;

pub use crate::engines::kvs::KvStore;
pub use crate::engines::sled::SledKvStore;
