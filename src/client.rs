use std::io::{Error, Result};
use std::net::TcpStream;

use serde::Serialize;
use slog::{info, Logger};

use crate::net::{GetRequest, KVRequest, Response, RmRequest, SetRequest};

pub struct KvsClient {
    addr: String,
    conn: Option<TcpStream>,
    logger: Logger,
}

///
/// KvsClient implementation which sends commands over the network to the target server
///
impl KvsClient {
    pub fn new(logger: Logger, addr: String) -> Result<KvsClient> {
        let conn = Some(TcpStream::connect(&addr)?);

        Ok(KvsClient { addr, conn, logger })
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.send_request(GetRequest { key })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.send_request(SetRequest { key, value })
    }

    pub fn rm(&mut self, key: String) -> Result<()> {
        self.send_request(RmRequest { key })
    }

    fn send_request<R: KVRequest + Serialize>(
        &mut self,
        request: R,
    ) -> Result<<<R as KVRequest>::Response as Response>::T> {
        info!(&self.logger, "Sending request"; "addr" => &self.addr);

        let mut conn = self.get_conn()?;
        bincode::serialize_into(&mut conn, &request.to_request())
            .map_err(|e| Error::other(e.to_string()))?;
        let response: R::Response =
            bincode::deserialize_from(&mut conn).map_err(|e| Error::other(e.to_string()))?;

        info!(&self.logger, "Received response");
        
        response.to_result()
    }

    fn get_conn(&mut self) -> Result<&mut TcpStream> {
        Ok(match self.conn {
            Some(ref mut conn) => conn,
            None => {
                let conn = TcpStream::connect(&self.addr)?;
                self.conn.get_or_insert(conn)
            }
        })
    }
}
