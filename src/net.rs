

use serde::{Deserialize, Serialize};

///
/// Message sent to server for each request
/// 
#[derive(Serialize, Deserialize)]
pub(crate) enum Request {
    Set {key: String, value: String},
    Get {key: String},
    Rm {key: String},
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

///
/// Response received back from server. Could be an error
/// 
#[derive(Serialize, Deserialize)]
pub(crate) enum SetResponse {
    Ok(()),
    Error(Exception)
}

///
/// Response received back from server. Could be an error
/// 
#[derive(Serialize, Deserialize)]
pub(crate) enum RmResponse {
    Ok(()),
    Error(Exception)
}
