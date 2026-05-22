use std::fmt;

#[derive(Debug)]
pub enum SwarmError {
    Db(String),
    Process(String),
    Io(std::io::Error),
    InvalidInput(String),
    AgentNotFound(String),
    AgentInactive { id: String, status: String },
    InvalidRequest(String),
    Timeout(String),
    Internal(String),
}

impl fmt::Display for SwarmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Db(msg) => write!(f, "database error: {msg}"),
            Self::Process(msg) => write!(f, "process error: {msg}"),
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::InvalidInput(msg) => write!(f, "invalid input: {msg}"),
            Self::AgentNotFound(id) => write!(f, "topic not found: {id}"),
            Self::AgentInactive { id, status } => {
                write!(
                    f,
                    "topic {id} is not accepting messages; status is {status}"
                )
            }
            Self::InvalidRequest(msg) => write!(f, "{msg}"),
            Self::Timeout(msg) => write!(f, "timeout: {msg}"),
            Self::Internal(msg) => write!(f, "internal error: {msg}"),
        }
    }
}

impl std::error::Error for SwarmError {}

impl From<std::io::Error> for SwarmError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<rusqlite::Error> for SwarmError {
    fn from(err: rusqlite::Error) -> Self {
        Self::Db(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, SwarmError>;
