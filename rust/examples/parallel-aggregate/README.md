# Ballista Example: Parallel Aggregate Query in Rust

*NOTE: this is a work-in-progress and is not functional yet*

This example shows how to manually create a Ballista cluster of Rust executors and run an aggregate query in parallel across those executors.

## Prerequisites

You will need a Kubernetes cluster to deploy to. I recommend using [Minikube](https://kubernetes.io/docs/tutorials/hello-minikube).

Ballista will need some permissions.

```bash
kubectl apply -f rbac.yaml
```

You will need to run [download-nyctaxi-files.sh](download-nyctaxi-files.sh) to download a subset of the [NYC Taxi data set](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and then edit [pv.yaml](pv.yaml) to provide the correct path to these files.

```bash
kubectl apply -f pv.yaml
```

## Build Example Docker Image

If you are using Minikube, make sure your docker environment is pointing to the Docker daemon running in Minikube.

```bash
eval $(minikube -p minikube docker-env)
```

From this directory.

```bash
./build-docker-image.sh
```

## Create Ballista Cluster

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