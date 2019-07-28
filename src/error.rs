//! Ballista error types

use std::io;
use std::result;

use datafusion::error::ExecutionError;
use k8s_openapi::http;
use reqwest;

pub type Result<T> = result::Result<T, BallistaError>;

/// Ballista error
#[derive(Debug)]
pub enum BallistaError {
    NotImplemented(String),
    General(String),
    DataFusionError(ExecutionError),
    ReqwestError(reqwest::Error),
    IoError(io::Error),
    HttpError(http::Error),
    KubeAPIRequestError(k8s_openapi::RequestError),
    KubeAPIResponseError(k8s_openapi::ResponseError),
}

impl From<String> for BallistaError {
    fn from(e: String) -> Self {
        BallistaError::General(e)
    }
}

impl From<ExecutionError> for BallistaError {
    fn from(e: ExecutionError) -> Self {
        BallistaError::DataFusionError(e)
    }
}

impl From<reqwest::Error> for BallistaError {
    fn from(e: reqwest::Error) -> Self {
        BallistaError::ReqwestError(e)
    }
}

impl From<io::Error> for BallistaError {
    fn from(e: io::Error) -> Self {
        BallistaError::IoError(e)
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
