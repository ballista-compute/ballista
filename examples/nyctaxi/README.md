# Ballista Tutorial

This tutorial walks through running the included nyc taxi example.

# Set up environment

- Create a minikube cluster

# Download data

```bash
./bin/download-yellow-2018.sh
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