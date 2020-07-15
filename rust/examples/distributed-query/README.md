# Distributed Query Execution

This example demonstrates running a distributed query against a local Ballista cluster.

## Using a local standalone cluster

### Prerequisites

etcd must be running locally.

### Start one or more executors

```bash
cargo run --release --bin executor -- --mode etcd --port 50051 --etcd-urls localhost:2379
cargo run --release --bin executor -- --mode etcd --port 50052 --etcd-urls localhost:2379
```

### Execute the query

The example will create a logical query plan and submit it to the cluster for execution. The executor receiving the 
query will create a physical plan and schedule execution in the cluster and then return the results.

```bash
cargo run
``` 

## Using Kubernetes

NOTE: these instructions are out of date and will be updated prior to the 0.3.0 release.

## Prerequisites

You will need to create a Ballista cluster in Kubernetes. This is documented in the [README](../../../kubernetes/README.md) in the top-level kubernetes folder. 

## Build Example Docker Image

If you are using Minikube, make sure your docker environment is pointing to the Docker daemon running in Minikube.

```bash
eval $(minikube -p minikube docker-env)
```

From this directory.

```bash
./build-docker-image.sh
```

## Deploy Example

Run the following kubectl command to deploy the example as a job.

```bash
kubectl apply -f parallel-aggregate-rs.yaml
```

Run the `kubectl get pods` again to find the pod for the example.

```bash
kubectl get pods
NAME                          READY   STATUS      RESTARTS   AGE
ballista-0                    1/1     Running     0          105m
ballista-1                    1/1     Running     0          105m
parallel-aggregate-rs-5vjj2   0/1     Completed   0          95m
```

Run the `kubectl logs` command to see the logs from the example.

```bash
kubectl logs parallel-aggregate-rs-5vjj2
```

## Teardown

Remove cluster:

```bash
kubectl delete -f parallel-aggregate-rs.yaml
kubectl delete -f cluster-deployment.yaml
```
