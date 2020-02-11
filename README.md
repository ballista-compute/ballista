# Ballista


[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Version](https://img.shields.io/crates/v/ballista.svg)](https://crates.io/crates/ballista)
[![Gitter Chat](https://badges.gitter.im/ballista-rs/community.svg)](https://gitter.im/ballista-rs/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

# Overview

Ballista is a proof-of-concept distributed compute platform based on [Kubernetes](https://kubernetes.io/) and [Apache Arrow](https://arrow.apache.org/). 

Note that the project has pivoted since the original PoC and is currently being re-implemented. The most significant change is that this is no longer a pure Rust project. I still believe that Rust is a great language for this project, but it can't be the only language. One of the key benefits of Arrow is that it supports multiple languages, including C, C++, C#, Go, Java, JavaScript, MATLAB, Python, R, Ruby, and Rust. It should therefore be possible for the Ballista architecture to support more than one language. User's need the ability to execute custom code as part of a distributed compute job and likely have existing code. User's are also likely to want compatibility with more traditional data science languages such as Python or R, as well as Java.

# Ballista Goals

- Define a physical query plan in protobuf format
- Use Apache Flight for sending query plans between nodes, and streaming results between nodes
- Allow clusters to be created, consisting of executors implemented in any language that supports Flight
- Distributed compute jobs should be capable of invoking code in more than one language (with some performance trade-offs for IPC overhead)
- Provide clients and connectors for Java, Rust, Python, and Apache Spark

# Ballista Anti Goals

- Ballista is not intended to replace Apache Spark but to augment it

# Status

I learned a lot from the initial PoC (see below for a demo and more info) but have decided to start the project again due to the changes in scope mentioned above.

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




