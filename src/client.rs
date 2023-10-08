use std::io::{BufReader, BufWriter, Error, Result};
use std::net::TcpStream;

use slog::{info, Logger};

use crate::net::{GetRequest, RmRequest, SetRequest, GetResponse, SetResponse, RmResponse};

pub struct KvsClient {
    addr: String,
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
    logger: Logger,
}

macro_rules! send_request {
    ($self:expr, $req: ident, $resp: ident, $($arg:tt),+) => {{
        info!(&$self.logger, "Sending request"; "addr" => &$self.addr);

        bincode::serialize_into(&mut $self.writer, &$req{$($arg),+})
            .map_err(|e| Error::other(e.to_string()))?;
        let response: $resp =
            bincode::deserialize_from(&mut $self.reader).map_err(|e| Error::other(e.to_string()))?;

        info!(&$self.logger, "Received response");

        match response {
            $resp::Ok(value) => Ok(value),
            $resp::Error(err) => Err(Error::other(err.what))
        }
    }};
}

///
/// KvsClient implementation which sends commands over the network to the target server
///
impl KvsClient {
    pub fn new(logger: Logger, addr: String) -> Result<KvsClient> {
        let conn = TcpStream::connect(&addr)?;

        Ok(KvsClient {
            addr,
            reader: BufReader::new(conn.try_clone()?),
            writer: BufWriter::new(conn),
            logger,
        })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        send_request!(self, GetRequest, GetResponse, key)
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        send_request!(self, SetRequest, SetResponse, key, value)
    }

    pub fn rm(&mut self, key: String) -> Result<()> {
        send_request!(self, RmRequest, RmResponse, key)
    }

}
