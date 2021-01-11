# Contributing to Ballista

## First steps

Please read my article [How to build a modern distributed compute platform](https://andygrove.io/how_to_build_a_modern_distributed_compute_platform/) 
since it is a good introduction to how I think Ballista (and other distributed compute platforms) should work. This 
article is a work in progress that I update from time to time, as I learn more about this subject, or when I feel 
motivated to write.

There is also a [wiki](https://github.com/ballista-compute/ballista/wiki) with a list of interesting reading material.

This project depends on some existing technologies, so it is a good idea to learn a little about those too:

- [Apache Arrow](https://arrow.apache.org/)
- [DataFusion](https://github.com/apache/arrow/tree/master/rust/datafusion)
- [Kubernetes](https://kubernetes.io/)
- [gRPC](https://grpc.io/)

Ballista will extend DataFusion to support distributed query execution of DataFusion queries by providing the following components:

- Serde code to support serialization and deserialization of logical and physical query plans in protocol buffer format (so that full or partial query plans can be sent between processes).
- Executor process implementing the Flight protocol that can receive a query plan and execute it.
- Shuffle write operator that can store the partitioned output of a query in the executor’s memory, or persist to the file system.
- Shuffle read operator than can read shuffle partitions from other executors in the cluster.
- Distributed query planner / scheduler that will start with a DataFusion physical plan and insert shuffle read and write operators as necessary and then execute the query stages.
- Kubernetes/Etcd support so that clusters can easily be created.

## Introduce Yourself!

We have a [Gitter IM room](https://gitter.im/ballista-rs/community) for discussing this project as well as a 
[discord channel](https://discord.gg/95PMxSk). 

## Issues

See the current milestones and issues 
[here](https://github.com/ballista-compute/ballista/milestones?direction=asc&sort=title&state=open). I recommend 
starting here when contributing because there is a plan in place for delivering useful point solutions along the way 
as the project heads towards a v1.0 release. 

## Creating Pull Requests

This project uses the standard [GitHub Forking Workflow](https://gist.github.com/Chaser324/ce0505fbed06b947d962).

## Development Environment

See the [developer docs](./docs/README.md) for instructions on setting up a local build environment.

