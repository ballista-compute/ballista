# Ballista - Rust
This crate contains the Ballista Executor & Scheduler.

## Run Executor
```bash
RUST_LOG=info cargo run --bin executor
...
[2021-02-11T05:30:13Z INFO  executor] Running with config: ExecutorConfig { host: "localhost", port: 50051, work_dir: "/var/folders/y8/fc61kyjd4n53tn444n72rjrm0000gn/T/.tmpv1LjN0", concurrent_tasks: 4 }
```
By default, the executor will bind to `localhost` and listen on port `50051`.

## Run Scheduler
```bash
$ RUST_LOG=info cargo run --release --bin scheduler
...
[2021-02-11T05:29:30Z INFO  scheduler] Ballista v0.4.1 Scheduler listening on 0.0.0.0:50050
[2021-02-11T05:30:13Z INFO  ballista::scheduler] Received register_executor request for ExecutorMetadata { id: "6d10f5d2-c8c3-4e0f-afdb-1f6ec9171321", host: "localhost", port: 50051 }
```

By default, the scheduler will bind to `localhost` and listen on port `50051`.

For example, please refer [here](../benchmarks/tpch/README.md).

