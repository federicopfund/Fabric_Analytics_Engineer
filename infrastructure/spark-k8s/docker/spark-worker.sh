#!/bin/bash


# Número de nodos trabajadores
NUM_WORKERS=5

# Esperar hasta que el nodo maestro esté disponible
while ! getent hosts spark-master; do
  sleep 5
done

# Configuración de Spark
SPARK_HOME=/opt/spark
SPARK_WORKER_DIR=/opt/spark/work

# Configuración avanzada de nodos trabajadores
for ((i=1; i<=NUM_WORKERS; i++)); do
  WORKER_LOG_DIR=$SPARK_WORKER_DIR/$i
  WORKER_PORT=$((8081 + i))
  
  mkdir -p $WORKER_LOG_DIR

  $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
    spark://spark-master:7077 \
    --webui-port $WORKER_PORT \
    --work-dir $WORKER_LOG_DIR \
    --cores 2 \  # Ajusta el número de núcleos por trabajador según tus necesidades
    --memory 8g \  # Ajusta la memoria asignada por trabajador según tus necesidades
    --executor-memory 6g \  # Ajusta la memoria asignada por executor según tus necesidades
    --executor-cores 2 \  # Ajusta el número de núcleos por executor según tus necesidades
    --conf spark.worker.cleanup.enabled=true \  # Habilita la limpieza de trabajadores muertos
    --conf spark.worker.cleanup.interval=3600 \  # Establece el intervalo de limpieza en segundos
    --conf spark.worker.cleanup.appDataTtl=86400 \  # Establece el tiempo de vida de datos de aplicaciones en segundos
    > $WORKER_LOG_DIR/worker.log 2>&1 &

  echo "Nodo trabajador $i iniciado en el puerto $WORKER_PORT."
done

# Configuración de memoria para PySpark
MEMORY='10g'
PYSPARK_SUBMIT_ARGS="--driver-memory $MEMORY pyspark-shell"
export PYSPARK_SUBMIT_ARGS

echo "Configuraciones avanzadas aplicadas."
