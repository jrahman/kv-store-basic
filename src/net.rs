use std::fmt::Display;

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
        match self {
            Request::Set(set) => f.write_fmt(format_args!("{:?}", set)),
            Request::Get(get) => f.write_fmt(format_args!("{:?}", get)),
            Request::Rm(rm) => f.write_fmt(format_args!("{:?}", rm)),
        }
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
        match self {
            GetResponse::Ok(_) => f.write_fmt(format_args!("GetResponse::Ok")),
            GetResponse::Error(err) => {
                f.write_fmt(format_args!("GetResponse::Error({})", err.what))
            }
        }
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
        match self {
            SetResponse::Ok(_) => f.write_fmt(format_args!("SetResponse::Ok")),
            SetResponse::Error(err) => {
                f.write_fmt(format_args!("SetResponse::Error({})", err.what))
            }
        }
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
        match self {
            RmResponse::Ok(_) => f.write_fmt(format_args!("RmResponse::Ok")),
            RmResponse::Error(err) => f.write_fmt(format_args!("RmResponse::Error({})", err.what)),
        }
    }
}
