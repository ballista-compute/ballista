# Ballista: Distributed Compute Platform

[![License][license-badge]][license-url]
[![Crates.io][crates-badge]][crates-url]
[![Discord chat][discord-badge]][discord-url]

[license-badge]: https://img.shields.io/badge/License-Apache%202.0-blue.svg
[license-url]: https://opensource.org/licenses/Apache-2.0
[crates-badge]: https://img.shields.io/crates/v/ballista.svg
[crates-url]: https://crates.io/crates/ballista
[discord-badge]: https://img.shields.io/discord/735486030626422884.svg?logo=discord&style=flat-square
[discord-url]: https://discord.gg/95PMxSk

## Overview

Ballista is a proof-of-concept distributed compute platform primarily implemented in Rust, using Apache Arrow as the 
memory model. It is built on an architecture that allows other programming languages to be supported as first-class 
citizens without paying a penalty for serialization costs.

## Status

With the release of Apache Arrow 3.0.0 there are many breaking changes in the Rust implementation and as a result it
has been necessary to comment out much of the code in this repository and gradually get each Rust module working again
with the 3.0.0 release.

**For the latest stable version of Ballista, see [branch-0.3](https://github.com/ballista-compute/ballista/tree/branch-0.3)**.

## Technologies

The foundational technologies in Ballista are:

- [Apache Arrow](https://arrow.apache.org/) memory model and compute kernels for efficient processing of data.
- [Apache Arrow Flight Protocol](https://arrow.apache.org/blog/2019/10/13/introducing-arrow-flight/) for efficient data 
transfer between processes.
- [Google Protocol Buffers](https://developers.google.com/protocol-buffers) for serializing query plans.
- [Docker](https://www.docker.com/) for packaging up executors along with user-defined code.

Ballista can be deployed in [Kubernetes](https://kubernetes.io/), or as a standalone cluster using 
[etcd](https://etcd.io/) for discovery.

## Architecture

The following diagram highlights some of the integrations that will be possible with this unique architecture. Note 
that not all components shown here are available yet.

![Ballista Architecture Diagram](docs/ballista-architecture.png)

## How does this compare to Apache Spark?

Although Ballista is largely inspired by Apache Spark, there are some key differences.

- The choice of Rust as the main execution language means that memory usage is deterministic and avoids the overhead of 
GC pauses.
- Ballista is designed from the ground up to use columnar data, enabling a number of efficiencies such as vectorized 
processing (SIMD and GPU) and efficient compression. Although Spark does have some columnar support, it is still 
largely row-based today.
- The combination of Rust and Arrow provides excellent memory efficiency and memory usage can be 5x - 10x lower than 
Apache Spark in some cases, which means that more processing can fit on a single node, reducing the overhead of 
distributed compute.
- The use of Apache Arrow as the memory model and network protocol means that data can be exchanged between executors 
in any programming language with minimal serialization overhead.

## Examples

The following examples should help illustrate the current capabilities of Ballista

- [TPC-H Benchmark](https://github.com/ballista-compute/ballista/tree/main/rust/examples/tpch)
- [Distributed query execution in Rust](https://github.com/ballista-compute/ballista/tree/main/rust/examples/distributed-query)
- [Rust bindings for Apache Spark](https://github.com/ballista-compute/ballista/tree/main/rust/examples/apache-spark-rust-bindings)

## Releases

Ballista releases are now available on [crates.io](https://crates.io/crates/ballista), 
[Maven Central](https://search.maven.org/search?q=g:org.ballistacompute) and 
[Docker Hub](https://hub.docker.com/u/ballistacompute). Please refer to the 
[user guide](https://ballistacompute.org/docs/) for instructions on using a released version of Ballista. 

## Documentation

The [user guide](https://ballistacompute.org/docs/) is hosted at [https://ballistacompute.org](https://ballistacompute.org/), 
along with the [blog](https://ballistacompute.org/) where news and release notes are posted.

Developer documentation can be found in the [docs](docs/README.md) directory.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for information on contributing to this project.
