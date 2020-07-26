# Deploying Ballista to Kubernetes

This document explains how to create a Ballista cluster of executors in Kubernetes.

## Prerequisites

You will need a Kubernetes cluster to deploy to. I recommend using 
[Minikube](https://kubernetes.io/docs/tutorials/hello-minikube) for local testing, or Amazon's Elastic Kubernetes 
Service (EKS). These instructions are for using Minikube on Ubuntu.

You will either need to use published Ballista Docker images or you will need to build Docker images locally.

Run the following command before building Docker images to ensure they go to the Docker context in Minikube.

```bash
eval $(minikube -p minikube docker-env)
```

## Create Minikube cluster

Create a Minikube cluster using the docker driver.

```bash
minikube start --driver=docker --cpus=12
```

## RBAC 

Ballista will need permissions to list pods. We will apply the following yaml to create `list-pods` cluster role and 
bind it to the default service account in the current namespace.

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: list-pods
rules:
- apiGroups:
  - ""
  resources:
  - pods
  verbs:
  - list
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ballista-list-pods
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: list-pods
subjects:
- kind: ServiceAccount
  name: default
  namespace: default
```

```bash
kubectl apply -f rbac.yaml
```

You should see the following output:

```bash
clusterrole.rbac.authorization.k8s.io/list-pods created
clusterrolebinding.rbac.authorization.k8s.io/ballista-list-pods created
```

## Mounting a volume

You will need to run [download-nyctaxi-files.sh](download-nyctaxi-files.sh) to download a subset of the [NYC Taxi data set](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) and then edit [pv.yaml](../../../kubernetes/pv.yaml) to provide the correct path to these files.

First, we need to mount the host path into the Minikube VM.

```bash
minikube mount /mnt/:/mnt/
```

You should see output like this:

```asm
Mounting host path /mnt/nyctaxi/ into VM as /mnt/nyctaxi ...
  Mount type:   <no value>
  User ID:      docker
  Group ID:     docker
  Version:      9p2000.L
  Message Size: 262144
  Permissions:  755 (-rwxr-xr-x)
  Options:      map[]
  Bind Address: 172.17.0.1:43715
    Userspace file server: ufs starting
Successfully mounted /mnt/nyctaxi/ to /mnt/nyctaxi
```

Next, we will apply the following yaml to create a persistent volume and a persistent volume claim so that the specified host directory is available to the containers.

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: nyctaxi-pv
  labels:
    type: local
spec:
  storageClassName: manual
  capacity:
    storage: 10Gi
  accessModes:
    - ReadWriteOnce
  hostPath:
    path: "/mnt/nyctaxi"
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: nyctaxi-pv-claim
spec:
  storageClassName: manual
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 3Gi
```

Create a persistent volume.

```bash
kubectl apply -f pv.yaml
```

You should see the following output:

```bash
persistentvolume/nyctaxi-pv created
persistentvolumeclaim/nyctaxi-pv-claim created
```

## Create Ballista Cluster

We will apply the following yaml to create a service and a stateful set of twelve Rust executors. Note that can you simply change the docker image name from `ballistacompute/ballista-rust` to `ballistacompute/ballista-jvm` or `ballistacompute/ballista-spark` to use the JVM or Spark executor instead. 

This definition will create twelve executors, using 1GB each. If you are running on a computer with limited memory available then you may want to reduce the number of replicas.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: ballista
  labels:
    app: ballista
spec:
  ports:
    - port: 50051
      name: flight
  clusterIP: None
  selector:
    app: ballista
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ballista
spec:
  serviceName: "ballista"
  replicas: 12
  selector:
    matchLabels:
      app: ballista
  template:
    metadata:
      labels:
        app: ballista
        ballista-cluster: ballista
    spec:
      containers:
      - name: ballista
        image: ballistacompute/ballista-rust:0.3.0-alpha-1
        resources:
          requests:
            cpu: "1"
            memory: "1024Mi"
          limits:
            cpu: "1"
            memory: "1024Mi"
        ports:
          - containerPort: 50051
            name: flight
        volumeMounts:
          - mountPath: /mnt/nyctaxi
            name: nyctaxi
      volumes:
      - name: nyctaxi
        persistentVolumeClaim:
          claimName: nyctaxi-pv-claim
```

Run the following kubectl command to deploy the Ballista cluster.

```bash
kubectl apply -f ballista-cluster-rust.yaml
```

You should see the following output:

```
service/ballista created
statefulset.apps/ballista created
```

Run the `kubectl get pods` command to confirm that the pods are running. It will take a few seconds for all of the pods to start.

```
kubectl get pods
NAME          READY   STATUS    RESTARTS   AGE
ballista-0    1/1     Running   0          37s
ballista-1    1/1     Running   0          33s
ballista-10   1/1     Running   0          16s
ballista-11   1/1     Running   0          15s
ballista-2    1/1     Running   0          32s
ballista-3    1/1     Running   0          30s
ballista-4    1/1     Running   0          28s
ballista-5    1/1     Running   0          27s
ballista-6    1/1     Running   0          24s
ballista-7    1/1     Running   0          22s
ballista-8    1/1     Running   0          20s
ballista-9    1/1     Running   0          18s
```

Run the `kubectl logs ballista-0` command to see logs from the first executor to confirm that the correct version is running and that there are no errors.

```
Ballista v0.3.0 Rust Executor listening on V4(0.0.0.0:50051)
```

## Port Forwarding

```bash
kubectl port-forward service/ballista 50051:50051
```

```
Forwarding from 127.0.0.1:50051 -> 50051
Forwarding from [::1]:50051 -> 50051
```

## Teardown

Run the following kubectl command to delete the Ballista cluster.

```bash
kubectl delete -f ballista-cluster-rust.yaml
```