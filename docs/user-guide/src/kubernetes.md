# Deploying Ballista with Kubernetes

**NOTE: These instructions need updating for 0.4.0-alpha-2**

You will need a Kubernetes cluster to deploy to. I recommend using 
[Minikube](https://kubernetes.io/docs/tutorials/hello-minikube) for local testing, or Amazon's Elastic Kubernetes Service (EKS). 

These instructions are for using Minikube on Ubuntu.

## Create a Minikube cluster

Create a Minikube cluster using the docker driver.

```bash
minikube start --driver=docker --cpus=12
```

## Permissions

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
  verbs: ["get", "watch", "list", "create", "edit", "delete"]
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

First, we need to mount the host data directory into the Minikube VM. This examples assumes that the local data 
directory is `/mnt/` and that we are going to mount it to the same path in the pod.

```bash
minikube mount /mnt:/mnt
```

You should see output similar to this:

```bash
Mounting host path /mnt/ into VM as /mnt ...
  Mount type:   <no value>
  User ID:      docker
  Group ID:     docker
  Version:      9p2000.L
  Message Size: 262144
  Permissions:  755 (-rwxr-xr-x)
  Options:      map[]
  Bind Address: 172.17.0.1:43715
    Userspace file server: ufs starting
Successfully mounted /mnt/ to /mnt
```

Next, we will apply the following yaml to create a persistent volume and a persistent volume claim so that the 
specified host directory is available to the containers.

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: data-pv
  labels:
    type: local
spec:
  storageClassName: manual
  capacity:
    storage: 10Gi
  accessModes:
    - ReadWriteOnce
  hostPath:
    path: "/mnt"
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data-pv-claim
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

## Creating the Ballista cluster

We will apply the following yaml to create a service and a stateful set of twelve Rust executors. Note that can you 
simply change the docker image name from `ballistacompute/ballista-rust` to `ballistacompute/ballista-jvm` 
or `ballistacompute/ballista-spark` to use the JVM or Spark executor instead. 

This definition will create six executors, using 1-2GB each. If you are running on a computer with limited memory 
available then you may want to reduce the number of replicas.

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
  replicas: 6
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
        image: ballistacompute/ballista-rust:0.4.0-alpha-2
        command: ["/executor"]
        args: ["--external-host=0.0.0.0", "--port=50051"]
        resources:
          requests:
            cpu: "1"
            memory: "1024Mi"
          limits:
            cpu: "2"
            memory: "2048Mi"
        ports:
          - containerPort: 50051
            name: flight
        volumeMounts:
          - mountPath: /mnt
            name: data
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: data-pv-claim
```

Run the following kubectl command to deploy the Ballista cluster.

```bash
kubectl apply -f ballista-cluster.yaml
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

## Port Forwarding

Run the following command to expose the service so that clients can submit queries to the cluster.

```bash
kubectl port-forward service/ballista 50051:50051
```

You should see the following output:

```
Forwarding from 127.0.0.1:50051 -> 50051
Forwarding from [::1]:50051 -> 50051
```

## Deleting the Ballista cluster

Run the following kubectl command to delete the cluster.

```bash
kubectl delete -f ballista-cluster.yaml
```