# Testing the Rust Executor

There are a number of ways to test Ballista after making code changes.

## Cargo Run

You can run an executor directly from the command-line without the need to build Docker images. This is the quickest
way to test code changes (other than writing unit tests).

The executors currently require etcd. The easiest way to run etcd is to use docker-compose. Simply create 
a `docker-compose.yaml` with the following content and then run `docker-compose up`.

```yaml
version: '2.0'
services:
  etcd:
    image: quay.io/coreos/etcd:v3.4.9
    command: "etcd -advertise-client-urls http://localhost:2379 -listen-client-urls http://0.0.0.0:2379"
    ports:
      - "2379:2379"
```
 
Run the following command from the top-level `rust` directory to run an executor.

```bash
cargo run --release --bin executor -- --mode etcd --port 50051 --concurrent-tasks 2
```

You can now go ahead and run one of the [examples](../rust/examples), either from the command-line using `cargo run` 
or direct from your IDE.

## Docker Compose

To test with docker-compose, it will first be necessary to first build a Docker image containing the executor.

```bash
./dev/build-rust.sh
```

Create a `docker-compose.yaml` based on the following example and customize it based on your environment.

```yaml
version: '2.0'
services:
  etcd:
    image: quay.io/coreos/etcd:v3.4.9
    command: "etcd -advertise-client-urls http://etcd:2379 -listen-client-urls http://0.0.0.0:2379"
    ports:
      - "2379:2379"
  ballista-rust:
    image: ballistacompute/ballista-rust:0.3.0-SNAPSHOT
    command: "/executor --mode etcd --etcd-urls etcd:2379 --external-host 0.0.0.0 --port 50051 --concurrent-tasks 2"
    ports:
      - "50051:50051"
    volumes:
      - /mnt/nyctaxi:/mnt/nyctaxi
```

Run the following command to launch the services.

```bash
docker-compose up
```

## Minikube

See instructions [here](../kubernetes/README.md).
