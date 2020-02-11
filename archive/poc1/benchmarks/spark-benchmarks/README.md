# Spark Benchmark

Download and install Apache Spark from https://spark.apache.org/downloads.html

Set `SPARK_HOME` env var to install location:

```bash
export SPARK_HOME=/home/andy/spark-2.4.3-bin-hadoop2.7/
```


Build the JAR.

`mvn package`

Use `kubectl` to find the URL for your Kubernetes cluster.

```bash
kubectl cluster-info
Kubernetes master is running at https://10.0.0.117:8443
KubeDNS is running at https://10.0.0.117:8443/api/v1/namespaces/kube-system/services/kube-dns:dns/proxy

To further debug and diagnose cluster problems, use 'kubectl cluster-info dump'.
```

Run `spark-submit` to deploy in cluster mode. See https://spark.apache.org/docs/latest/running-on-kubernetes.html for more info.

NOTE: this isn't quite working ... but should be close ... need to figure out how to provide jar location, probably with volume mount to hostPath 

```bash
$SPARK_HOME/bin/spark-submit \
  --class io.andygrove.ballista.spark.Main \
  --master k8s://https://10.0.0.117:8443 \
  --deploy-mode cluster \
  --executor-memory 1G \
  --conf spark.executor.instances=2 \
  --conf spark.kubernetes.container.image=andygrove/spark:2.4.3 \
  --conf spark.kubernetes.driver.volumes.hostPath.jar.mount.path=/mnt/spark-benchmarks-1.0-SNAPSHOT.jar \
  --conf spark.kubernetes.driver.volumes.hostPath.jar.options.path=`pwd`/target/spark-benchmarks-1.0-SNAPSHOT.jar \
  --conf spark.kubernetes.driver.volumes.hostPath.data.mount.path=/mnt/ssd/nyc_taxis/csv \
  --conf spark.kubernetes.driver.volumes.hostPath.data.options.path=/mnt/ssd/nyc_taxis/csv \
    local:///mnt/spark-benchmarks-1.0-SNAPSHOT.jar  
```