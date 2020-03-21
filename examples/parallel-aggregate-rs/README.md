# Ballista Example: Parallel Aggregate Query in Rust

*NOTE: this is a work-in-progress and is not functional yet*

This example shows how to manually create a Ballista cluster of Rust executors and run an aggregate query in parallel across those executors.

## Prerequisites

You will need a Kubernetes cluster to deploy to. I recommend using [Minikube](https://kubernetes.io/docs/tutorials/hello-minikube).

```bash
minikube start --vm-driver=podman
```

You will need the NYC Taxi data set available in the cluster and you will need to edit the [cluster-deployment.yaml](cluster-deployment.yaml) to provide the correct path to that data.

## Build Ballista Rust Docker Images

If you are using Minikube, make sure your docker environment is pointing to the Docker daemon running in Minikube.

```bash
eval $(minikube -p minikube docker-env)
```

From the root of the project.

```bash
docker build -t ballistacompute/rust-base -f docker/rust-base.dockerfile .
docker build -t ballistacompute/rust -f docker/rust.dockerfile .
```

## Build Docker Image for Example

From this directory.

```bash
./build-docker-image.sh
```

## Create Ballista cluster in k8s

Run the following kubectl command to deploy the ballista cluster.

```bash
kubectl apply -f cluster-deployment.yaml
```

You should see the following output:

```
service/ballista created
statefulset.apps/ballista created
```

Run the `kubectl get pods` command to confirm that the pods are running.

```
kubectl get pods
NAME         READY   STATUS    RESTARTS   AGE
ballista-0   1/1     Running   0          44s
ballista-1   1/1     Running   0          41s
```

## Deploy Example

Run the following kubectl command to deploy the example as a job.

```bash
kubectl apply -f parallel-aggregate-rs.yaml
```

Run the following command to view the logs.

```bash
kubectl logs 
```


## Teardown

Remove cluster:

```bash
kubectl delete -f cluster-deployment.yaml
```