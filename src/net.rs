use std::fmt::Display;
use std::io::Result;
use std::io::Error;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub(crate) trait Response {
    type T;
    
    fn to_result(self) -> Result<Self::T>;
}

pub(crate) trait KVRequest {
    fn to_request(self) -> Request;

    type Response: Response + DeserializeOwned;
}

///
/// Message sent to server for each request
///
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Request {
    Set(SetRequest),
    Get(GetRequest),
    Rm(RmRequest),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SetRequest {
    pub(crate) key: String,
    pub(crate) value: String,
}

impl KVRequest for SetRequest {
    fn to_request(self) -> Request {
        Request::Set(self)
    }

    type Response = SetResponse;
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct GetRequest {
    pub(crate) key: String,
}

impl KVRequest for GetRequest {
    fn to_request(self) -> Request {
        Request::Get(self)
    }

    type Response = GetResponse;
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RmRequest {
    pub(crate) key: String,
}

impl KVRequest for RmRequest {
    fn to_request(self) -> Request {
        Request::Rm(self)
    }

    type Response = RmResponse;
}

impl Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self))
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Exception {
    pub(crate) what: String,
}

///
/// Response received back from server. Could be an error
///
#[derive(Serialize, Deserialize)]
pub(crate) enum GetResponse {
    Ok(Option<String>),
    Error(Exception),
}

impl Response for GetResponse {
    type T = Option<String>;
    
    fn to_result(self) -> Result<Self::T> {
        match self {
            Self::Ok(value) => Ok(value),
            Self::Error(err) => Err(Error::other(err.what))
        }
    }
}

impl Display for GetResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self))
    }
}

///
/// Response received back from server. Could be an error
///
#[derive(Serialize, Deserialize)]
pub(crate) enum SetResponse {
    Ok(()),
    Error(Exception),
}

impl Response for SetResponse {
    type T = ();

    fn to_result(self) -> Result<Self::T> {
        match self {
            Self::Ok(value) => Ok(value),
            Self::Error(err) => Err(Error::other(err.what))
        }
    }
}

impl Display for SetResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self))
    }
}

///
/// Response received back from server. Could be an error
///
#[derive(Serialize, Deserialize)]
pub(crate) enum RmResponse {
    Ok(()),
    Error(Exception),
}

impl Response for RmResponse {
    type T = ();
    
    fn to_result(self) -> Result<Self::T> {
        match self {
            Self::Ok(value) => Ok(value),
            Self::Error(err) => Err(Error::other(err.what))
        }
    }
}

impl Display for RmResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self))
    }
}
