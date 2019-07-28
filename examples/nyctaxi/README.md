# Ballista Tutorial

This tutorial walks through running the included nyc taxi example.

## Set up environment

- Create a minikube cluster
- Create required RBAC roles/rolebindings and persistent volume: `kubectl create -f ./templates`
- Install ingress controller (https://kubernetes.io/docs/tasks/access-application-cluster/ingress-minikube/#enable-the-ingress-controller)

## Download data

Download the CSV files and store in a directory. This directory will need to be mounted into minikube so that the Ballista executors can access it.

In a new terminal:

```bash
./bin/download-yellow-2018.sh
mkdir -p data
mv yellow_tripdata_* data
minikube mount $PWD/data:/mnt/ssd/nyc_taxis/csv
```

## Run

From the root of this project, run these commands:

**TODO - update the images mentioned below so these commands work.**

```bash
cargo run --bin ballista -- create-cluster --name nyctaxi --num-executors 12 --volumes nyctaxi:/mnt/ssd/nyc_taxis/csv --image andygrove/ballista:0.2.0
cargo run --bin ballista -- run --name nyctaxi --image andygrove/ballista-nyctaxi:0.2.0
cargo run --bin ballista -- delete-cluster --name nyctaxi
kubectl delete job ballista-nyctaxi-app
```

## Notes

```bash
docker tag ballista-nyctaxi:latest andygrove/ballista-nyctaxi:0.1.3
docker push andygrove/ballista-nyctaxi:0.1.3

docker tag ballista:latest andygrove/ballista:0.1.3
docker push andygrove/ballista:0.1.3
```
