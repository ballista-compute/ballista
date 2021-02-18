# Installing Ballista with Docker Compose

Docker Compose is a convenient way to launch a cluister when testing locally. The following Docker Compose example 
demonstrates how to start a cluster using a single process that acts as both a scheduler and an executor, with a data 
volume mounted into the container so that Ballista can access the host file system.

```yaml
version: '2.0'
services:
  etcd:
    image: quay.io/coreos/etcd:v3.4.9
    command: "etcd -advertise-client-urls http://etcd:2379 -listen-client-urls http://0.0.0.0:2379"
    ports:
      - "2379:2379"
  ballista-executor:
    image: ballistacompute/ballista-rust:0.4.0-alpha-2
    command: "/executor --bind-host 0.0.0.0 --port 50051 --local"
    environment:
      - RUST_LOG=info
    ports:
      - "50050:50050"
      - "50051:50051"
    volumes:
      - ./data:/data


```

With the above content saved to a `docker-compose.yaml` file, the following command can be used to start the single 
node cluster.

```bash
docker-compose up
```

The scheduler listens on port 50050 and this is the port that clients will need to connect to.
