fn main() {
    prost_build::compile_protos(&["../proto/ballista.proto"], &["../proto"])
        .unwrap_or_else(|e| panic!("protobuf compilation failed: {}", e));

    // tower_grpc_build::Config::new()
    //     .enable_server(true)
    //     .enable_client(true)
    //     .build(&["../proto/ballista.proto"], &["../proto"])
    //     .unwrap_or_else(|e| panic!("protobuf compilation failed: {}", e));
}
