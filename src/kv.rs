use std::collections::HashMap;
use std::io::{ErrorKind, Result};
use std::path::PathBuf;

use crate::log::LogOperation::{self, Rm, Set};
use crate::log::{Log, LogRecord};
use crate::kvs::KvsEngine;

use slog::{o, Logger, info};

///
pub struct KvStore {
    log: Log,

    ///
    /// Stores our mapping from the key as a string to the index in the log
    /// where the value will be found. A separate lookup into the log is
    /// required to read the value.
    ///
    mapping: HashMap<String, u64>,

    logger: Option<Logger>,
}

impl KvStore {
    ///
    /// Create a new KvStore implementation which is empty. Key-value pairs
    /// will be added later using the public APIs
    ///
    pub fn open(logger: Option<Logger>, path: PathBuf) -> Result<KvStore> {
        let mut log = Log::open(logger.clone().map(|l| l.new(o!("module" => "log"))), path)?;
        let mut mapping: HashMap<String, u64> = HashMap::new();

        for record in log.iter()? {
            if let Ok(rec) = record {
                match rec.operation {
                    Rm { key } => mapping.remove(&key),
                    Set { key, .. } => mapping.insert(key, rec.index),
                };
            } else {
                return Err(record.unwrap_err());
            }
        }

        Ok(KvStore {
            logger,
            log,
            mapping,
        })
    }

    pub fn compact(&mut self) -> Result<()> {
        self.log.compact_log(|record: &LogRecord| -> bool {
            match record.operation {
                Rm { ref key } => self.mapping.contains_key(key),
                Set { ref key, .. } => !self.mapping.contains_key(key),
            }
        })
    }
}

impl KvsEngine for KvStore {

    ///
    ///
    ///
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let op = LogOperation::Set {
            key: key.to_string(),
            value,
        };
        let position = self.log.write(op)?;
        self.mapping.insert(key.to_string(), position);

        let total_size: u64 = self.log.total_size()?;
        if total_size > 2048 {
            if let Some(ref logger) = self.logger {
                info!(logger, "Trigging compaction"; "total_size" => total_size);
            }
            self.log.compact_log(|elem| {
                match &elem.operation {
                    Set { key, .. } => {
                        self.mapping.contains_key(key) && *self.mapping.get(key).unwrap() == elem.index
                    }
                    Rm { key } => {
                        !self.mapping.contains_key(key)
                    }
                }
            })?;
        }

        Ok(())
    }

    ///
    fn get(&mut self, key: String) -> Result<Option<String>> {
        let position = self.mapping.get(&key);
        match position {
            Some(position) => {
                let record = self.log.read(*position)?;
                match record.operation {
                    Set { key: _, value } => Ok(Some(value)),
                    Rm { key: _ } => Err(ErrorKind::InvalidData.into()),
                }
            }
            None => Ok(None),
        }
    }

    ///
    ///
    ///
    fn remove(&mut self, key: String) -> Result<()> {
        self.log.write(LogOperation::Rm {
            key: key.to_string(),
        })?;
        self.mapping.remove(&key);
        Ok(())
    }
}
