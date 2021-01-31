## Creating a Ballista cluster using etcd

Ballista can use etcd for discovery, making it easy to create a cluster on a local development environment, or on 
any networked computers.

# Installing etcd

Please refer to the [etcd](https://etcd.io/) web site for installation instructions. Etcd version 3.4.9 or later is 
recommended.

# Installing Ballista

Simply start one or more executors using the following syntax:

```bash
cargo run --release --bin executor -- --mode etcd --etcd-urls localhost:2379 --external-host localhost --port 50051 
cargo run --release --bin executor -- --mode etcd --etcd-urls localhost:2379 --external-host localhost --port 50052 
```

The external host and port will be registered in etcd and other executors will attempt to connect using those details.
 