use std::collections::HashMap;
use std::io::{ErrorKind, Result};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use crate::engines::KvsEngine;
use crate::log::LogOperation::{self, Rm, Set};
use crate::log::{Log, LogRecord};

use slog::{o, Logger};

struct State {
    log: Log,

    ///
    /// Stores our mapping from the key as a string to the index in the log
    /// where the value will be found. A separate lookup into the log is
    /// required to read the value.
    ///
    mapping: Mutex<HashMap<String, u64>>,

    logger: Option<Logger>,
}

///
pub struct KvStore {
    state: Arc<State>,
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
            state: Arc::new(State {
                logger,
                log,
                mapping: Mutex::new(mapping),
            }),
        })
    }

    pub fn compact(&mut self) -> Result<()> {
        self.state.log.compact_log(|record: &LogRecord| -> bool {
            match record.operation {
                Rm { ref key } => self.state.mapping.lock().unwrap().contains_key(key),
                Set { ref key, .. } => !self.state.mapping.lock().unwrap().contains_key(key),
            }
        })
    }
}

impl KvsEngine for KvStore {
    ///
    ///
    ///
    fn set(&self, key: String, value: String) -> Result<()> {
        let op = LogOperation::Set {
            key: key.to_string(),
            value,
        };
        let position = self.state.log.write(op)?;
        self.state.mapping.lock().unwrap().insert(key.to_string(), position);

        // let total_size: u64 = self.log.total_size()?;
        // if total_size > 2048 {
        //     if let Some(ref logger) = self.logger {
        //         info!(logger, "Trigging compaction"; "total_size" => total_size);
        //     }
        //     self.log.compact_log(|elem| {
        //         match &elem.operation {
        //             Set { key, .. } => {
        //                 self.mapping.contains_key(key) && *self.mapping.get(key).unwrap() == elem.index
        //             }
        //             Rm { key } => {
        //                 !self.mapping.contains_key(key)
        //             }
        //         }
        //     })?;
        // }

        Ok(())
    }

    ///
    fn get(&self, key: String) -> Result<Option<String>> {
        match self.state.mapping.lock().unwrap().get(&key) {
            Some(position) => {
                let record = self.state.log.read(*position)?;
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
    fn remove(&self, key: String) -> Result<()> {
        self.state.log.write(LogOperation::Rm {
            key: key.to_string(),
        })?;
        self.state.mapping.lock().unwrap().remove(&key);
        Ok(())
    }
}

impl Clone for KvStore {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone()
        }
    }
}
