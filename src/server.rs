use slog::{info, Logger};
use std::io::Result;
use std::net::{TcpListener, TcpStream};

use crate::engine::KvsEngine;

pub struct KvsServer<Engine: KvsEngine> {
    addr: String,
    logger: Logger,
    engine: Engine
}

impl<Engine: KvsEngine + Sync> KvsServer<Engine> {
    pub fn new(addr: String, logger: Logger, engine: Engine) -> KvsServer<Engine> {
        KvsServer { addr, logger, engine }
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

    fn process_connection(&self, connection: TcpStream) -> Result<()> {
        info!(self.logger, "Received connection"; "remote_addr" => connection.peer_addr()?.to_string());
        loop {}
    }
}
