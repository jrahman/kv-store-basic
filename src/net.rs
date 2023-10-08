use std::fmt::Display;
use std::io::Result;
use std::io::Error;

use serde::{Deserialize, Serialize};

///
/// Message sent to server for each request
///
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Request {
    Set(SetRequest),
    Get(GetRequest),
    Rm(RmRequest),
}

impl From<GetRequest> for Request {
    fn from(value: GetRequest) -> Self {
        Request::Get(value)
    }
}

impl From<SetRequest> for Request {
    fn from(value: SetRequest) -> Self {
        Request::Set(value)
    }
}

impl From<RmRequest> for Request {
    fn from(value: RmRequest) -> Self {
        Request::Rm(value)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SetRequest {
    pub(crate) key: String,
    pub(crate) value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct GetRequest {
    pub(crate) key: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RmRequest {
    pub(crate) key: String,
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

impl Display for RmResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self))
    }
}
