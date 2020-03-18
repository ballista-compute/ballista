# Ballista Example: Parallel Aggregate Query in Rust

This example shows how to manually create a Ballista cluster of Rust executors and run an aggregate query in parallel across those executors.

## Prerequisites

- Kubernetes cluster (minikube)
- Sample data installed (TBD)

## Step 1: Build Docker Image for Ballista Rust Project

From the root of the project.

```bash
cd rust
./build-docker-image.sh
```

## Step 2: Build Docker Image for Example

From this directory.

```bash
./build-docker-image.sh
```

## Step 3: Create Ballista cluster in k8s

```bash
kubectl apply -f cluster-deployment.yaml
```

## Step 4: Deploy Example