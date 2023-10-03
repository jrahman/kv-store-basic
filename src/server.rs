use slog::{info, Logger};
use std::io::Result;
use std::net::{TcpListener, TcpStream};

pub struct KvsServer {
    addr: String,
    logger: Logger,
}

impl KvsServer {
    pub fn new(addr: String, logger: Logger) -> KvsServer {
        KvsServer { addr, logger }
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
