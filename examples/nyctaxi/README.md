# Ballista Tutorial

This tutorial walks through running the included nyc taxi example.

# Set up environment

- Create a minikube cluster
- Install ingress controller (https://kubernetes.io/docs/tasks/access-application-cluster/ingress-minikube/#enable-the-ingress-controller)
- Run `kubectl proxy` so that Kubernetes API is accessible on localhost:8001

# Download data

Download the CSV files and store in a directory. This directory will need to be mounted into minikube so that the Ballista executors can access it.

In a new terminal:

```bash
./bin/download-yellow-2018.sh
mkdir -p data
mv yellow_tripdata_* data
minikube mount $PWD/data:/mnt/ssd/nyc_taxis/csv
```

# Run

From the root of this project, run these commands:

```bash
cargo run --bin ballista -- create-cluster --name nyctaxi --num-executors 12 --template examples/nyctaxi/templates/executor.yaml
cargo run --bin ballista -- run --name nyctaxi --template examples/nyctaxi/templates/application.yaml
cargo run --bin ballista -- delete-cluster --name nyctaxi
kubectl delete job ballista-nyctaxi-app 
```

# Notes

```bash
docker tag ballista-nyctaxi:latest andygrove/ballista-nyctaxi:0.1.3
docker push andygrove/ballista-nyctaxi:0.1.3

docker tag ballista:latest andygrove/ballista:0.1.3
docker push andygrove/ballista:0.1.3
```
