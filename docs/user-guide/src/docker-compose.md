# Installing Ballista with Docker Compose

*NOTE: Ballista 0.4.0 is still under development and there are no published Docker images yet. Please refer to the
[developer documentation](https://github.com/ballista-compute/ballista/tree/main/docs) for instructions on building 
Docker images locally.*

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
  ballista-scheduler:
    image: ballistacompute/ballista-rust:0.4.0-alpha-1
    command: "/scheduler --config-backend etcd --etcd-urls etcd:2379 --bind-host 0.0.0.0 --port 50050"
    ports:
      - "50050:50050"
    depends_on:
      - etcd
  ballista-executor:
    image: ballistacompute/ballista-rust:0.4.0-alpha-1
    command: "/executor --bind-host 0.0.0.0 --port 50051 --scheduler-host ballista-scheduler"
    ports:
      - "50051:50051"
    volumes:
      - ./data:/data
    depends_on:
      - ballista-scheduler
```

With the above content saved to a `docker-compose.yaml` file, the following command can be used to start the single node 
cluster.

```bash
docker-compose up
```

Note that the executor will not be able to execute queries until it successfully registers with the scheduler.

