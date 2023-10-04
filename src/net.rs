

use std::fmt::Display;

use serde::{Deserialize, Serialize};

///
/// Message sent to server for each request
/// 
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Request {
    Set {key: String, value: String},
    Get {key: String},
    Rm {key: String},
}

impl Display for Request {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self))
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) struct Exception {
    pub(crate) what: String
}

///
/// Response received back from server. Could be an error
/// 
#[derive(Serialize, Deserialize)]
pub(crate) enum GetResponse {
    Ok(Option<String>),
    Error(Exception)
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
    Error(Exception)
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
    Error(Exception)
}

impl Display for RmResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self))
    }
}
