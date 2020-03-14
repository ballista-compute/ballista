//! Ballista is a proof-of-concept distributed compute platform based on Kubernetes and Apache Arrow.

// include the generated protobuf source as a submodule
#[allow(clippy::all)]
pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/ballista.protobuf.rs"));
}

pub mod dfcontext;
pub mod error;
pub mod logical_plan;
pub mod serde;
