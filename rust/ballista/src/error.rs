//! Ballista error types

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::io;
use std::result;

use crate::arrow::error::ArrowError;
use crate::datafusion::error::ExecutionError;

pub type Result<T> = result::Result<T, BallistaError>;

/// Ballista error
#[derive(Debug)]
pub enum BallistaError {
    NotImplemented(String),
    General(String),
    ArrowError(ArrowError),
    DataFusionError(ExecutionError),
    IoError(io::Error),
    ReqwestError(reqwest::Error),
    HttpError(http::Error),
    KubeAPIRequestError(k8s_openapi::RequestError),
    KubeAPIResponseError(k8s_openapi::ResponseError),
    // TonicError(tonic::status::Status)
}

pub fn ballista_error(message: &str) -> BallistaError {
    BallistaError::General(message.to_owned())
}

impl From<String> for BallistaError {
    fn from(e: String) -> Self {
        BallistaError::General(e)
    }
}

impl From<ArrowError> for BallistaError {
    fn from(e: ArrowError) -> Self {
        BallistaError::ArrowError(e)
    }
}

impl From<ExecutionError> for BallistaError {
    fn from(e: ExecutionError) -> Self {
        BallistaError::DataFusionError(e)
    }
}

impl From<io::Error> for BallistaError {
    fn from(e: io::Error) -> Self {
        BallistaError::IoError(e)
    }
}

impl From<reqwest::Error> for BallistaError {
    fn from(e: reqwest::Error) -> Self {
        BallistaError::ReqwestError(e)
    }
}

impl From<http::Error> for BallistaError {
    fn from(e: http::Error) -> Self {
        BallistaError::HttpError(e)
    }
}

impl From<k8s_openapi::RequestError> for BallistaError {
    fn from(e: k8s_openapi::RequestError) -> Self {
        BallistaError::KubeAPIRequestError(e)
    }
}

impl From<k8s_openapi::ResponseError> for BallistaError {
    fn from(e: k8s_openapi::ResponseError) -> Self {
        BallistaError::KubeAPIResponseError(e)
    }
}

// impl From<tonic::status::Status> for BallistaError {
//     fn from(e: tonic::status::Status) -> Self {
//         BallistaError::TonicError(e)
//     }
// }

impl Display for BallistaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            BallistaError::NotImplemented(ref desc) => write!(f, "Not implemented: {}", desc),
            BallistaError::General(ref desc) => write!(f, "General error: {}", desc),
            BallistaError::ArrowError(ref desc) => write!(f, "Arrow error: {}", desc),
            BallistaError::DataFusionError(ref desc) => write!(f, "DataFusion error: {:?}", desc),
            BallistaError::IoError(ref desc) => write!(f, "IO error: {}", desc),
            BallistaError::ReqwestError(ref desc) => write!(f, "Reqwest error: {}", desc),
            BallistaError::HttpError(ref desc) => write!(f, "HTTP error: {}", desc),
            BallistaError::KubeAPIRequestError(ref desc) => {
                write!(f, "KubeAPI request error: {}", desc)
            }
            BallistaError::KubeAPIResponseError(ref desc) => {
                write!(f, "KubeAPI response error: {}", desc)
            }
        }
    }
}

impl Error for BallistaError {}
