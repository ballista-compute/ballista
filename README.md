# Ballista

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Version](https://img.shields.io/crates/v/ballista.svg)](https://crates.io/crates/ballista)
[![Gitter Chat](https://badges.gitter.im/ballista-rs/community.svg)](https://gitter.im/ballista-rs/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

# Overview

Ballista is an experimental distributed compute platform based on [Kubernetes](https://kubernetes.io/) and [Apache Arrow](https://arrow.apache.org/) that I am developing in my spare time as a way to learn more about distributed data processing. It is largely inspired by Apache Spark.

Ballista aims to be language-agnostic with an architecture that is capable of supporting any language supported by Apache Arrow, which currently includes C, C++, C#, Go, Java, JavaScript, MATLAB, Python, R, Ruby, and Rust. 

# Ballista Goals

- Define a logical query plan in protobuf format. See [ballista.proto](proto/ballista.proto)
- Provide DataFrame style interfaces for JVM (Java, [Kotlin](jvm/client/src/main/kotlin/DataFrame.kt), Scala), [Rust](rust/src/dataframe.rs), and Python
- Provide a JDBC Driver, allowing Ballista to be used from existing BI and SQL tools
- Use Apache Flight for sending query plans between nodes, and streaming results between nodes
- Allow clusters to be created, consisting of executors implemented in any language that supports Flight
- Distributed compute jobs should be capable of invoking code in more than one language (with some performance trade-offs for IPC overhead)
- Provide integrations with Apache Spark (e.g. Spark V2 Data Source allowing Spark to interact with Ballista)

# Ballista Anti Goals

- Ballista is not intended to replace Apache Spark but to augment it

# Status

I learned a lot from the initial PoC (see below for a demo and more info) but have decided to start the project again due to the changes in scope mentioned above so the project is currently in a state of flux and nothing works right now but I am in the process of building a second PoC.

Here is a rough plan for delivering PoC #2:

- [ ] Implement a Rust server implementing Flight protocol that can receive a logical plan and validate it and execute it (in progress)
- [ ] Implement a Kotlin DataFrame client that can build a plan and execute it against the Rust server (in progress)
- [ ] Implement a Rust DataFrame client that can build a plan and execute it against the Rust server (in progress)
- [ ] Implement a JDBC driver that can execute a SQL statement against the Rust server (in progress)
- [ ] Implement a Scala server implementing the Flight protocol that can receive a logical plan and translate it to Spark and execute it
- [ ] Build a benchmark client in Kotlin that can run against the Rust and Scala servers

# PoC #1

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

Note that PoC #1 is now archived [here](archive/poc1).

# Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for information on contributing to this project.




