# Installing Ballista with Docker Compose

Docker Compose is a convenient way to launch executors when testing locally. This example `docker-compose.yaml` 
demonstrates how to start a Ballista Rust executor and how to mount a data volume into the container.

```yaml
version: '2.0'
services:
  etcd:
    image: quay.io/coreos/etcd:v3.4.9
    command: "etcd -advertise-client-urls http://etcd:2379 -listen-client-urls http://0.0.0.0:2379"
    ports:
      - "2379:2379"
  ballista-rust:
    image: ballistacompute/ballista-rust:0.3.0
    command: "/executor --mode etcd --etcd-urls etcd:2379 --external-host 0.0.0.0 --port 50051 --concurrent-tasks=2"
    ports:
      - "50051:50051"
    volumes:
      - /mnt:/mnt
```

With the above content saved to a `docker-compose.yaml` file, the following command can be used to start the single node 
cluster.

```bash
docker-compose up
```

Noet that the executor will not be able to execute queries until it succcesfully registers with etcd.

