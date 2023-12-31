use slog::{error, info, Logger};
use std::io::{BufReader, BufWriter, Error, Result, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Mutex;

use crate::engines::KvsEngine;
use crate::net::{Exception, GetResponse, Request, RmResponse, SetResponse};

pub struct KvsServer<Engine: KvsEngine> {
    addr: String,
    logger: Logger,
    engine: Mutex<Engine>,
}

impl<Engine: KvsEngine + Sync + Send> KvsServer<Engine> {
    pub fn new(addr: String, logger: Logger, engine: Engine) -> KvsServer<Engine> {
        KvsServer {
            addr,
            logger,
            engine: Mutex::new(engine),
        }
    }

    ///
    /// Main event processing loop for all operations on the server. Handles
    /// inbound connections and spwans threads to process each connection in
    /// parallel
    ///
    pub fn run(&mut self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr)?;

        info!(self.logger, "Starting server"; "addr" => &self.addr);

        std::thread::scope(|s| -> Result<()> {
            for stream in listener.incoming() {
                let connection = stream?;
                s.spawn(|| {
                    match self.process_connection(connection) {
                        Ok(()) => {},
                        Err(err) => error!(self.logger, "Error while processing connection"; "error" => err.to_string())
                    }
                });
            }
            Ok(())
        })
    }

    fn process_connection(&self, connection: TcpStream) -> Result<()> {
        let peer_addr = connection.peer_addr()?.to_string();

        info!(self.logger, "Received connection"; "remote_addr" => &peer_addr);

        let mut reader = BufReader::new(connection.try_clone()?);
        let mut writer = BufWriter::new(connection);

        macro_rules! send_response {
            ($resp:expr) => {{
                let resp = $resp;
                bincode::serialize_into(&mut writer, &resp).map_err(|e| Error::other(e))?;
                writer.flush()?;
                info!(self.logger, "Sent response"; "remote_addr" => &peer_addr, "response" => format!("{}", resp));
            }};
        }

        loop {
            info!(self.logger, "Waiting for request");

            let request: Request =
                bincode::deserialize_from(&mut reader).map_err(|e| Error::other(e.to_string()))?;

            info!(self.logger, "Received request"; "remote_addr" => &peer_addr, "request" => format!("{}", request));

            match request {
                Request::Set(cmd) => {
                    send_response!(match self.engine.lock().unwrap().set(cmd.key, cmd.value) {
                        Ok(value) => SetResponse::Ok(value),
                        Err(err) => SetResponse::Error(Exception {
                            what: err.to_string()
                        }),
                    })
                }
                Request::Get(cmd) => {
                    send_response!(match self.engine.lock().unwrap().get(cmd.key) {
                        Ok(value) => GetResponse::Ok(value),
                        Err(err) => GetResponse::Error(Exception {
                            what: err.to_string()
                        }),
                    })
                }
                Request::Rm(cmd) => {
                    send_response!(match self.engine.lock().unwrap().remove(cmd.key) {
                        Ok(value) => RmResponse::Ok(value),
                        Err(err) => RmResponse::Error(Exception {
                            what: err.to_string()
                        }),
                    })
                }
            };
        }
    }
}
