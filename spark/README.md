# Ballista integration with Apache Spark

This project contains:

- Ballista Spark Executor
- Spark V2 Connector for Ballista

## Ballista Spark Executor

Executor implementing the Ballista protocol (Apache Flight + protobuf-encoded Ballista query plans), allowing Spark to be used from any language supported by Ballista, including Java, Kotlin, Scala, and Rust.

## Spark V2 Connector for Ballista

The goal for this component is to allow Spark to interact with Ballista executors (Rust, Kotlin, and Spark executors exist).


