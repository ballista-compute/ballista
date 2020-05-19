//! Ballista is a proof-of-concept distributed compute platform based on Kubernetes and Apache Arrow.

pub use arrow;
pub use datafusion;

// include the generated protobuf source as a submodule
#[allow(clippy::all)]
pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/ballista.protobuf.rs"));
}

pub const BALLISTA_VERSION: &'static str = env!("CARGO_PKG_VERSION");

pub mod client;
pub mod cluster;
pub mod dataframe;
pub mod error;
pub mod plan;
pub mod serde;
pub mod utils;
