//! Ballista is a proof-of-concept distributed compute platform based on Kubernetes and Apache Arrow.

// include the generated protobuf source as a submodule
// https://github.com/tower-rs/tower-grpc/issues/194
#[allow(clippy::all)]
pub mod protobuf {
    include!(concat!(env!("OUT_DIR"), "/ballista.protobuf.rs"));
}

pub mod dataframe;
pub mod serde;