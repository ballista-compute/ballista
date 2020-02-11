fn main() {
    tower_grpc_build::Config::new()
        .enable_server(true)
        .enable_client(true)
        .build(&["proto/ballista/ballista.proto"], &["proto/ballista"])
        .unwrap_or_else(|e| panic!("protobuf compilation failed: {}", e));
}
