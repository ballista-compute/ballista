# Deploying Ballista to Kubernetes

This document explains how to create a Ballista cluster in Kubernetes.

## Generic kubernetes

This guide assumes that we have a kubernetes cluster and our role has permissions to:

* list and create pods
* list and create secrets
* list and create persistent volumes and claims
* create ClusterRole and ClusterRoleBinding

on the namespace `default`.

### Deploy volumes and write data to it

For this guide, we will create a persistent volume and claim and write data to it, which we can then query through Ballista.
To do so, run

```bash
kubectl apply -f kubernetes/pv.yaml
```

and check that its status and the claim status is "Bound":

```
kubectl get pv
kubectl get pvc
```

Next, we will spawn a pod to download data from s3 and write to the persistent volume:

```bash
kubectl apply -f kubernetes/populate-data.yaml
```

This operation takes some time as we have some data to download. This is done when the pod `populate-data` on `kubectl get pods`
shows status `Completed`. We can't deploy the cluster before this step is completed, as the volume can only be claimed by
a single pod at the time. Feel free to change [populate-data.yaml](./populate-data.yaml) to dump a smaller CSV.

### Setup credentials for pulling images

Development builds of the Ballista docker images are hosted on Github. It is necessary to set up authentication to be
able to use these images. Use the following command to create a Kuberneted secret containing your github credentials.  

_Note that we also have publicly available Docker images hosted on [Dockerhub](https://hub.docker.com/u/ballistacompute) 
for released versions of Ballista and these do not require authentication._


```bash
kubectl create secret docker-registry github \
    --docker-server=docker.pkg.github.com \
    --docker-username=$GITHUB_USERNAME \
    --docker-password=$GITHUB_TOKEN \
    --docker-email=$GITHUB_EMAIL
```

The value provided for `$GITHUB_TOKEN` must be an OAuth token associated with the Github account that belongs 
to `$GITHUB_USERNAME`. To generate a Github token, visit [https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token) 
and create a token that has the `read:registry` permission.

### Give the cluster permissions to list pods

```bash
kubectl apply -f kubernetes/rbac.yaml
```

This gives all pods running on namespace `default` permissions to list pods from the cluster's API
and is necessary for scheduling.

### Deploy Ballista

Once `populate-data` completes, we can bring the cluster up. For this, run

```bash
kubectl apply -f kubernetes/ballista-cluster.yaml
```

We can check the status with `kubectl get pods` and `kubectl logs ballista-0`.
This deploys two replicas and a service on port `50051`.
At this point, we can connect remotely with this port to run queries against the cluster.

A simple method to connect to it is to port-forward to our local machine:

```bash
kubectl port-forward service/ballista 50051:50051
```

It is now possible to run clients that use the DataFrame API to build queries and submit them to the cluster. The clients
should connect to `localhost:50051` to take advantage of the port forwarding to the cluster.

For example, it is now possible to run [this example](../rust/examples/distributed-query) to query the cluster, using
the following commands: 

```bash
cd $BALLISTA_HOME/rust/examples/distributed-query
cargo run /mnt/nyctaxi/csv --host localhost --port 50051
```

_Note that when querying Parquet files, it is currently required that the client also has access to the same 
data files with the same path because the DataFrame API will actually open the files to inspect the schema. This 
restriction will be removed in a future release._

### Customization

The procedure above is a minimal setup to deploy ballista on kubernetes. From here, we can just change any configuration above
to our needs. In particular, we can use the image name from `ballistacompute/ballista-rust` to `ballistacompute/ballista-jvm` or `ballistacompute/ballista-spark` to use a different executor.

## Deploy on minikube

For local development, we can also use minikube. We can set it up exactly the same way, but since 
minikube runs locally, we can simplify the deployment a bit:

First, we can use docker images from our own docker deamon,

```bash
eval $(minikube -p minikube docker-env)
minikube start --driver=docker
```

which does not require setting up a github access token.

Secondly, we can use a local mounted volume before creating a persistent volume:

```bash
minikube mount /mnt/:/mnt/
```

This way, we can download the data to our computer and place it in the mounted volume.

## Teardown

To revert all the steps above, we run `kubectl delete -f` on each of the files above. To delete the secret used
to download images, run

```bash
kubectl delete secret github
```
