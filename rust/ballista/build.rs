fn main() {
    prost_build::compile_protos(&["proto/ballista.proto"], &["proto"])
        .unwrap_or_else(|e| panic!("protobuf compilation failed: {}", e));
}
