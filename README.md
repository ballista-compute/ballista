# Ballista


[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Version](https://img.shields.io/crates/v/ballista.svg)](https://crates.io/crates/ballista)
[![Build Status](https://travis-ci.com/andygrove/ballista.svg?branch=master)](https://travis-ci.com/andygrove/ballista)
[![Gitter Chat](https://badges.gitter.im/ballista-rs/community.svg)](https://gitter.im/ballista-rs/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Ballista is a proof-of-concept distributed compute platform based on Kubernetes and the Rust implementation of [Apache Arrow](https://arrow.apache.org/).

This is not my first attempt at building something like this. I originally wanted [DataFusion](https://github.com/apache/arrow/tree/master/rust/datafusion) to be a distributed compute platform but this was overly ambitious at the time, and it ended up becoming an in-memory query execution engine for the Rust implementation of Apache Arrow. However, DataFusion now provides a good foundation to have another attempt at building a [modern distributed compute platform](https://andygrove.io/how_to_build_a_modern_distributed_compute_platform/) in Rust.

My goal is to use this repo to move fast and try out ideas that eventually can be contributed back to Apache Arrow and to help drive requirements for Apache Arrow and DataFusion.

# Demo

This demo shows a Ballista cluster being created in Minikube and then shows the [nyctaxi example](examples/nyctaxi) being executed, causing a distributed query to run in the cluster, with each executor pod performing an aggregate query on one partition of the data, and then the driver merges the results and runs a secondary aggregate query to get the final result. 

[![asciicast](https://asciinema.org/a/SArI3f8PVFjgc45wHubEQQnca.svg)](https://asciinema.org/a/UCdmelZpxeACYVSeAlGHSWBRr)

Here are the commands being run, with some explanation:

```bash
# create a cluster with 12 executors
cargo run --bin ballista -- create-cluster --name nyctaxi --num-executors 12 --template examples/nyctaxi/templates/executor.yaml

# check status
kubectl get pods

# run the nyctaxi example application, that executes queries using the executors
cargo run --bin ballista -- run --name nyctaxi --template examples/nyctaxi/templates/application.yaml

# check status again to find the name of the application pod
kubectl get pods

# watch progress of the application
kubectl logs -f ballista-nyctaxi-app-n5kxl
```

# PoC Status

- [X] README describing project
- [X] Define service and minimal query plan in protobuf file
- [X] Generate code from protobuf file
- [X] Implement skeleton gRPC server
- [X] Implement skeleton gRPC client
- [X] Client can send query plan
- [X] Server can receive query plan
- [X] Server can translate protobuf query plan to DataFusion query plan
- [X] Server can execute query plan using DataFusion
- [X] Create Dockerfile for server
- [X] Ballista CLI - create cluster
- [X] Ballista CLI - delete cluster
- [X] Ballista CLI - run application
- [X] Simple example application works end to end
- [X] Add support for aggregate queries 
- [X] Server can return Arrow data back to the application (in CSV format for now)
- [X] Example application can aggregate the aggregate results from each partition/node
- [X] Write blog post and announce Ballista

# v1.0.0 Plan 

- [ ] Distributed query planner
- [ ] Implement support for all DataFusion logical plan and expressions
- [ ] Server can write results to CSV files
- [ ] Server can write results to Parquet files
- [ ] Implement Flight protocol
- [ ] Support user code as part of distributed query execution
- [ ] Interactive SQL support
- [ ] Java bindings (supporting Java, Kotlin, Scala)

# Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for information on contributing to this project.




