

use serde::{Deserialize, Serialize};

///
/// Message sent to server for each request
/// 
#[derive(Serialize, Deserialize)]
enum Request {
    Set {key: String, value: String},
    Get {key: String},
    Rm {key: String},
}

#[derive(Serialize, Deserialize)]
struct Exception {
    what: String
}

///
/// Response received back from server. Could be an errors
/// 
#[derive(Serialize, Deserialize)]
enum Response {
    Set,
    Get {value: String},
    Rm,
    Error(Exception)
}
