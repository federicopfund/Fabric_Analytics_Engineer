#!/bin/bash


echo "$(hostname -i) spark-master" >> /etc/hosts

# Opciones avanzadas de configuración para el nodo maestro
MASTER_OPTS="--conf spark.some.config.option=value
             --conf spark.another.config.option=another_value
             --conf spark.executor.memory=4g
             --conf spark.executor.cores=2
             --conf spark.master.rest.enabled=true"

/opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
  --ip spark-master \
  --port 7077 \
  --webui-port 8080 \
  $MASTER_OPTS
