#!/bin/bash

$SPARK_HOME/bin/spark-submit \
    --master k8s://https://172.17.0.2:8443 \
    --deploy-mode cluster \
    --name ballista-benchmarks \
    --class org.ballistacompute.spark.benchmarks.tpch.Tpch \
    --conf spark.kubernetes.driver.podTemplateFile=pod-template.yaml \
    --conf spark.kubernetes.executor.podTemplateFile=pod-template.yaml \
    --conf spark.executor.instances=12 \
    --conf spark.kubernetes.container.image=ballistacompute/spark-benchmarks:0.3.0-SNAPSHOT \
    local:////opt/ballista/jars/benchmarks.jar