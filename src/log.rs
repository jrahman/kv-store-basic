use slog::Logger;
use slog::*;
use std::collections::{BTreeMap, HashMap};
use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Result, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

extern crate slog;
extern crate slog_async;
extern crate slog_term;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub(crate) enum LogOperation {
    Set { key: String, value: String },
    Rm { key: String },
}

///
/// Represents a log record which is serialized and deserialzed to and from
/// log files stored on disk as persistent storage. Each record has a
/// monotonically increasing index number which denotes it's position in the
/// log address space. The log implementation handles the mapping between the
/// index and log file + offset into the file. The mapping from index -> offset
/// is split into two parts:
/// * index range -> log file
/// * log file -> offset in log file
///
/// Updates to the first mapping are made fast by only storing the lowest index
/// each log file stores, and using an ordered BTreeMap for lookups. Otherwise
/// a HashMap for said mapping would require significant overhead on compaction
/// to re-write the index -> log file mapping for each and every active key
///
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct LogRecord {
    pub(crate) index: u64,
    pub(crate) operation: LogOperation,
}

///
/// Header indicating the start of a file manifest
///
#[derive(Serialize, Deserialize)]
struct FileManifestHeader {
    magic_number: u64,
    entry_count: u16,
}

///
/// Record recording a log file, including it's file_number and the maximum
/// index in the file (or 0 if not yet sealed). Only 1 file should have
/// index = 0 and that file is the active file being appended to
///
#[derive(Serialize, Deserialize, Copy, Clone)]
struct FileManifestRecord {
    file_number: u16,
    max_index: u64,
    min_index: u64,
}

///
/// In-memory representation for a log file, including metadata on the file
/// itself such as the maximum index and the file number
///
struct LogFile {
    // Metadata on the file manifest contents for this log file
    manifest_record: FileManifestRecord,

    // Mapping from log index to offset into the file
    index_map: HashMap<u64, u64>,

    // Pointer to a File object which can be used to read/write from/to the
    // file as needed
    file: Arc<File>,

    logger: Option<Logger>,

    path: PathBuf,
}

impl LogFile {
    ///
    /// Open an existing log file given the FileManifestRecord describing
    /// the file to open and the base path to the directory storing the log
    ///
    /// During the open operation, the log files must be scanned sequentially
    /// to rebuild the index -> offset mapping used for lookups at runtime
    ///
    fn open(
        logger: &Option<Logger>,
        path: PathBuf,
        manifest_record: FileManifestRecord,
    ) -> Result<LogFile> {
        let log_file_path = path.join(format!("{}.log", manifest_record.file_number));

        if let Some(logger) = logger {
            info!(logger, "Opening log file"; "file_name" => log_file_path.to_str());
        }

        let mut file = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&log_file_path)?,
        );

        // Upon scanning, build a map from index -> offset for each record in the file
        file.seek(SeekFrom::Start(0))?;

        if let Some(logger) = logger {
            info!(logger, "Scanning log file"; "file_name" => log_file_path.to_str());
        }

        let mut index_map: HashMap<u64, u64> = HashMap::new();
        loop {
            let offset = file.stream_position()?;
            if offset == file.stream_len()? {
                break;
            }
            let log_record: LogRecord =
                bincode::deserialize_from(&mut file).map_err(|e| Error::other(e))?;
            index_map.insert(log_record.index, offset);
        }

        Ok(LogFile {
            path,
            manifest_record,
            file,
            index_map,
            logger: logger
                .clone()
                .map(|l| l.new(o!("file_name" => log_file_path.to_string_lossy().to_string()))),
        })
    }

    ///
    /// Create a new log file based on the specification described by the
    /// provided FileManifestRecord. The max_index will be u64::MAX while
    /// min_index will be the next index to be inserted into the log file
    /// upon a successful write
    ///
    fn create(
        logger: &Option<Logger>,
        path: PathBuf,
        manifest_record: FileManifestRecord,
    ) -> Result<LogFile> {
        let log_file_path = path.join(manifest_record.file_number.to_string());
        Ok(LogFile {
            path,
            manifest_record,
            index_map: HashMap::new(),
            file: Arc::new(File::create(&log_file_path)?),
            logger: logger
                .clone()
                .map(|l| l.new(o!("file_name" => log_file_path.to_string_lossy().to_string()))),
        })
    }

    ///
    /// Read a log record from the file based on the LogRecord's index. Will
    /// return an error if the index is not present in this log file
    ///
    fn read(&mut self, index: u64) -> Result<LogRecord> {
        if let Some(ref logger) = self.logger {
            info!(logger, "Reading record"; "index" => index);
        }

        self.file.seek(SeekFrom::Start(
            *self.index_map.get(&index).ok_or(ErrorKind::NotFound)?,
        ))?;
        Ok(bincode::deserialize_from(&*self.file).map_err(|e| Error::other(e.to_string()))?)
    }

    ///
    /// Write a new log record into the tail of the file
    ///
    fn write(&mut self, record: LogRecord) -> Result<()> {
        if let Some(ref logger) = self.logger {
            info!(logger, "Writing record"; "index" => record.index);
        }

        let offset = self.file.seek(SeekFrom::End(0))?;
        let index = record.index;
        bincode::serialize_into(self.file.as_ref(), &record)
            .map_err(|e| Error::other(e.to_string()))?;
        self.index_map.insert(index, offset);
        self.manifest_record.max_index = self.manifest_record.max_index.max(index);

        if let Some(ref logger) = self.logger {
            info!(logger, "Wrote record"; "index" => record.index);
        }

        Ok(())
    }

    fn size(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    ///
    /// Perform compaction on the file to shrink it. The predicate provided is
    /// used to determine if a log record should or should not remain in the
    /// file
    ///
    fn compact<F: Fn(&LogRecord) -> bool>(&mut self, predicate: &F) -> Result<()> {
        if let Some(ref logger) = self.logger {
            info!(logger, "Compacting file"; "file_number" => self.manifest_record.file_number);
        }

        let new_file_name = self
            .path
            .join(format!("{}.log.tmp", self.manifest_record.file_number));
        let old_file_name = self
            .path
            .join(format!("{}.log", self.manifest_record.file_number));

        let mut output_file = File::options()
            .write(true)
            .create(true)
            .open(&new_file_name)?;

        let file_iterator = FileIterator::new(self)?;

        for record in file_iterator {
            match record {
                Ok((record, _)) => {
                    if predicate(&record) {
                        bincode::serialize_into(&output_file, &record)
                            .map_err(|e| Error::other(e))?;
                    }
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }

        let new_size = output_file.stream_len()?;

        if let Some(ref logger) = self.logger {
            info!(logger, "Compacted log file"; "file_name" => old_file_name.to_str(), "original_size" => self.file.stream_len()?, "new_size" => new_size);
        }

        std::fs::rename(&old_file_name, &new_file_name)?;
        self.file = Arc::new(File::open(old_file_name)?);
        Ok(())
    }
}

///
/// Implements iteration over a single log file
///
struct FileIterator<'a> {
    // Pointer to the file to be read from
    log_file: Arc<File>,

    // Next offset in the file to read a record from
    iter: std::collections::hash_map::Iter<'a, u64, u64>,
}

impl<'a> FileIterator<'a> {
    fn new(log_file: &LogFile) -> Result<FileIterator> {
        Ok(FileIterator {
            log_file: log_file.file.clone(),
            iter: log_file.index_map.iter(),
        })
    }
}

impl<'a> Iterator for FileIterator<'a> {
    type Item = Result<(LogRecord, u64)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iter.next() {
            Some((_, offset)) => match self.log_file.seek(SeekFrom::Start(*offset)) {
                Ok(_) => {
                    let record: LogRecord = bincode::deserialize_from(&mut self.log_file).unwrap();
                    Some(Ok((record, *offset)))
                }
                Err(err) => Some(Err(err)),
            },
            None => None,
        }
    }
}

///
/// Abstraction over a set of files representing a log. Handles writes to the
/// log, and general log management
///
#[derive()]
pub(crate) struct Log {
    // Ordered log files, in increasing index number, the last file in the
    // BTreeMap is the current file being appended into. The key is the
    // smallest index associated with the LogFile
    log_files: BTreeMap<u64, LogFile>,

    // Next index number for the next write to the log
    next_index: AtomicU64,

    logger: Option<Logger>,

    path: PathBuf,
}

impl Log {
    ///
    /// Open an existing log or create a new one using a specific directory as defined by path
    ///
    pub(crate) fn open(logger: Option<Logger>, path: PathBuf) -> Result<Self> {
        // Mapping from first index in the log file, to the LogFile itself
        let mut log_files: BTreeMap<u64, LogFile> = BTreeMap::new();

        let mut manifest_records = Self::read_manifest(&logger, path.clone())?;

        // Upon the first load, create an empty manifest and add a single log file to it
        if manifest_records.is_empty() {
            let log_file_path = path.join("0.log");
            if let Some(ref logger) = logger {
                info!(logger, "Creating new log file"; "file" => log_file_path.to_str());
            }

            File::create(log_file_path)?;
            manifest_records.push(FileManifestRecord {
                file_number: 0,
                max_index: u64::MAX,
                min_index: u64::MIN,
            });
            Self::write_manifest(&logger, manifest_records.clone(), &path)?;
        }

        for record in manifest_records {
            let log_file = LogFile::open(&logger, path.clone(), record)?;
            log_files.insert(log_file.manifest_record.min_index, log_file);
        }

        let mut last_index = log_files
            .last_key_value()
            .map_or(0, |(_, log_file)| log_file.manifest_record.max_index);

        // The final file must be scanned over if it was a partial file open
        // upon last process exit. In such a case, the max_index = u64::max
        // indicating the file was not closed gracefully with a manifest write
        // with the up to date values
        if let Some(entry) = log_files.last_entry() {
            last_index = entry.get().manifest_record.max_index;
        }

        if let Some(ref logger) = logger {
            info!(logger, "Completed manifest scan"; "max_index" => last_index);
        }

        Ok(Self {
            log_files,
            next_index: AtomicU64::new(last_index + 1),
            logger,
            path,
        })
    }

    ///
    /// Read a LogRecord from the underlying file at a given location
    ///
    pub(crate) fn read(&mut self, index: u64) -> Result<LogRecord> {
        if let Some(ref logger) = self.logger {
            info!(logger, "Reading log"; "index" => index);
        }
        match self
            .log_files
            .lower_bound_mut(std::ops::Bound::Included(&index))
            .value_mut()
        {
            Some(entry) => Ok(entry.read(index)?),
            None => Err(Error::other("Failed to find file for index")),
        }
    }

    ///
    /// Write a new log record into the log. Returns the index in the log at
    /// which the record was written
    ///
    pub(crate) fn write(&mut self, operation: LogOperation) -> Result<u64> {
        if let Some(mut entry) = self.log_files.last_entry() {
            let tail_file = entry.get_mut();

            let record = LogRecord {
                index: self
                    .next_index
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                operation,
            };

            if let Some(ref logger) = self.logger {
                info!(logger, "Writing record"; "index" => record.index, "file_number" => tail_file.manifest_record.file_number);
            }

            let last_index = record.index;
            tail_file.write(record)?;

            // Force data to disk for durability prior to returning back to the caller
            tail_file.file.sync_data()?;

            if let Some(ref logger) = self.logger {
                info!(logger, "Wrote record"; "index" => last_index, "file_number" => tail_file.manifest_record.file_number);
            }

            Ok(last_index)
        } else {
            if let Some(ref logger) = self.logger {
                info!(logger, "Missing tail file");
            }
            Err(ErrorKind::InvalidData.into())
        }
    }

    ///
    /// Read the current manifest file returning a vector of FileManifestRecords
    /// sorted by max_index. This will read from the MANIFEST file in the
    /// target directory
    ///
    fn read_manifest(logger: &Option<Logger>, path: PathBuf) -> Result<Vec<FileManifestRecord>> {
        let manifest_file_path = path.join("MANIFEST");

        if let Some(logger) = logger {
            info!(logger, "Opening manifest"; "path" => manifest_file_path.to_str());
        }

        match File::open(&manifest_file_path) {
            Ok(mut file) => {
                let mut records: Vec<FileManifestRecord> = Vec::new();
                let header: FileManifestHeader = bincode::deserialize_from(&mut file).unwrap();

                if let Some(logger) = logger {
                    info!(logger, "Manifest file opened"; "entries" => header.entry_count);
                }

                for _i in 0..header.entry_count {
                    let manifest_entry: FileManifestRecord =
                        bincode::deserialize_from(&mut file).unwrap();
                    records.push(manifest_entry);
                }

                // Order according to the highest index number in the file, with the
                // exception that a single file with index MAX_INT indicates that file is
                // the active file, and thus is being written to
                records.sort_by_key(|a| a.max_index);

                if let Some(logger) = logger {
                    info!(logger, "Manifest parsed"; "entries" => records.len());
                }
                Ok(records)
            }
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(Vec::new()),
            Err(err) => {
                return Err(err);
            }
        }
    }

    ///
    /// Serialize the manifest onto the local file system. First writes it out
    /// to a MANIFEST.new file, and then later does an atomic rename to ensure
    /// a consistent view of the file is persisted
    ///
    fn write_manifest(
        logger: &Option<Logger>,
        mut records: Vec<FileManifestRecord>,
        path: &PathBuf,
    ) -> Result<()> {
        // Order according to the highest version number in the file, with the
        // exception that a single file with version 0 indicates that file is
        // the active file, and thus is being written to
        records.sort_by_key(|a| a.max_index);

        let new_manifest_file_path = path.join("MANIFEST.new");

        if let Some(ref logger) = logger {
            info!(logger, "Writing new MANIFEST"; "file_name" => new_manifest_file_path.to_str());
        }

        let w = File::create(&new_manifest_file_path)?;

        let header = FileManifestHeader {
            entry_count: records.len() as u16,
            magic_number: 0xDEAD_BEEF,
        };

        if let Some(ref logger) = logger {
            info!(logger, "Writing header into MANIFEST"; "file_name" => new_manifest_file_path.to_str());
        }

        bincode::serialize_into(&w, &header).unwrap();

        for record in records {
            if let Some(ref logger) = logger {
                info!(logger, "Writing record into MANIFEST"; "file_name" => new_manifest_file_path.to_str());
            }

            bincode::serialize_into(&w, &record).unwrap();
        }

        if let Some(ref logger) = logger {
            info!(logger, "Syncing manifest file"; "file_name" => new_manifest_file_path.to_str());
        }

        w.sync_all()?;

        if let Some(ref logger) = logger {
            info!(logger, "Finished syncing file"; "file_name" => new_manifest_file_path.to_str());
        }

        if let Some(ref logger) = logger {
            info!(logger, "Renaming file"; "source_file" => "MANIFEST.new", "destination_file" => "MANIFEST");
        }
        std::fs::rename(path.join("MANIFEST.new"), path.join("MANIFEST"))?;

        if let Some(ref logger) = logger {
            info!(logger, "Renamed file"; "source_file" => "MANIFEST.new", "destination_file" => "MANIFEST");
        }

        Ok(())
    }

    pub(crate) fn iter(&mut self) -> Result<LogIterator> {
        Ok(LogIterator::new(self)?)
    }

    ///
    /// Issue a compaction against the log to eliminate old records. The
    /// predicate passed should return true for records which should be
    /// retained, and false for records which should be dropped from the log.
    ///
    pub(crate) fn compact_log<F: Fn(&LogRecord) -> bool>(&mut self, predicate: F) -> Result<()> {
        if let Some(ref logger) = self.logger {
            info!(logger, "Starting compaction");
        }

        for log_file in self.log_files.values_mut() {
            log_file.compact(&predicate)?;
        }

        if let Some(ref logger) = self.logger {
            info!(logger, "Finished compaction");
        }

        // Once compaction has completed, write out the updated manifest with any updates
        Self::write_manifest(
            &self.logger,
            self.log_files
                .iter()
                .map(|(_, log_file)| log_file.manifest_record)
                .collect(),
            &self.path,
        )
    }

    ///
    /// Close the final file, flushing the manifest
    ///
    fn seal_last_file(&mut self) -> Result<()> {
        if let Some(mut entry) = self.log_files.last_entry() {
            let log_file = entry.get_mut();
            log_file.manifest_record.max_index =
                self.next_index.load(std::sync::atomic::Ordering::SeqCst) - 1;

            let mut last_record = log_file.manifest_record;
            last_record.file_number += 1;
            last_record.min_index = last_record.max_index + 1;
            last_record.max_index = u64::MAX;
            let first_index = last_record.min_index;

            self.log_files.insert(
                first_index,
                LogFile::create(&self.logger, self.path.clone(), last_record)?,
            );

            // Flush the manifest so the log files are picked up on a reload
            // after this point, including the updated max_index for the
            // previous last file, and the newly added tail file
            Self::write_manifest(
                &self.logger,
                self.log_files
                    .iter()
                    .map(|(_, log_file)| log_file.manifest_record)
                    .collect(),
                &self.path,
            )?;
        }
        Ok(())
    }
}

impl Drop for Log {
    fn drop(&mut self) {
        // Write manifest out as best effort, recovery process on startup can
        // properly scan a final file which was not sealed with a final
        // version number on shutdown.
        let records = self
            .log_files
            .iter()
            .map(|(_, log_file)| log_file.manifest_record)
            .collect();
        let _ = Self::write_manifest(&self.logger, records, &self.path);
    }
}

///
/// Iterator over all records in the log, starting at the first record.
/// Provides sequential access to all records in the log. Version numbers
/// will be strictly increasing for each log record returned, though gaps
/// are likely to occur due to compaction on the log
///
pub(crate) struct LogIterator<'a> {
    file_iterator: std::collections::btree_map::Iter<'a, u64, LogFile>,
    record_iterator: FileIterator<'a>,
}

impl<'a> LogIterator<'a> {
    fn new(log: &'a mut Log) -> Result<LogIterator> {
        let mut file_iterator = log.log_files.iter();
        let record_iterator = FileIterator::new(&mut file_iterator.next().unwrap().1)?;
        Ok(LogIterator {
            record_iterator,
            file_iterator,
        })
    }
}

impl<'a> Iterator for LogIterator<'a> {
    type Item = Result<LogRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        // Move to next file (or stop iteration)
        match self.record_iterator.next() {
            Some(value) => Some(value.map(|(log_record, _)| log_record)),
            None => {
                // Move to next file in the log
                // TODO address a bug here with missing values on log file transition
                match self.file_iterator.next() {
                    Some((_, mut log_file)) => match FileIterator::new(&mut log_file) {
                        Ok(record_iterator) => {
                            self.record_iterator = record_iterator;
                            self.record_iterator
                                .next()
                                .map(|v| v.map(|(log_record, _)| log_record))
                        }
                        Err(err) => Some(Err(err)),
                    },
                    None => None,
                }
            }
        }
    }
}
