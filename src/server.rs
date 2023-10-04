use slog::{info, Logger};
use std::io::{BufReader, BufWriter, Error, ErrorKind, Result, Write};
use std::net::{TcpListener, TcpStream};

use crate::engine::KvsEngine;
use crate::net::{GetResponse, Request, SetResponse, RmResponse, Exception};

pub struct KvsServer<Engine: KvsEngine> {
    addr: String,
    logger: Logger,
    engine: Engine,
}

impl<Engine: KvsEngine + Sync> KvsServer<Engine> {
    pub fn new(addr: String, logger: Logger, engine: Engine) -> KvsServer<Engine> {
        KvsServer {
            addr,
            logger,
            engine,
        }
    }

    ///
    /// Main event processing loop for all operations on the server. Handles
    /// inbound connections and spwans threads to process each connection in
    /// parallel
    ///
    pub fn run(&mut self) -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:6379")?;

        info!(self.logger, "Starting server"; "addr" => &self.addr);

        std::thread::scope(|s| -> Result<()> {
            for stream in listener.incoming() {
                let connection = stream?;
                s.spawn(|| self.process_connection(connection));
            }
            Ok(())
        })
    }

    fn process_connection(&mut self, connection: TcpStream) -> Result<()> {
        info!(self.logger, "Received connection"; "remote_addr" => connection.peer_addr()?.to_string());

        let reader = BufReader::new(&connection);
        let mut writer = BufWriter::new(&connection);

        macro_rules! send_response {
            ($resp:expr) => {{
                let resp = $resp;
                bincode::serialize_into(&mut writer, &resp).map_err(|e| Error::other(e))?;
                writer.flush()?;
            }};
        }

        let request: Request =
            bincode::deserialize_from(reader).map_err(|e| Error::other(e.to_string()))?;
        match request {
            Request::Set { key, value } => send_response!(match self.engine.set(key, value) {
                Ok(()) => SetResponse::Ok(()),
                Err(err) => SetResponse::Error(Exception{what: err.to_string()}),
            }),
            Request::Get { key } => send_response!(match self.engine.get(key) {
                Ok(value) => GetResponse::Ok(value),
                Err(err) => GetResponse::Error(Exception{what: err.to_string()}),
            }),
            Request::Rm { key } => send_response!(match self.engine.remove(key) {
                Ok(_) => RmResponse::Ok(()),
                Err(err) => RmResponse::Error(Exception{what: err.to_string()}),
            }),
        };

        Ok(())
    }
}
