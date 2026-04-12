#!/bin/bash
set -e

# Start SSH service
service ssh start

# Determine role based on container hostname
HOSTNAME=$(hostname)

case "$HOSTNAME" in
  namenode)
    echo ">>> Starting NameNode..."
    if [ ! -f /hadoop/dfs/name/current/VERSION ]; then
      echo ">>> Formatting NameNode..."
      hdfs namenode -format -force -nonInteractive
    fi
    hdfs namenode &

    # Wait for NameNode to be ready and leave safe mode
    echo ">>> Waiting for NameNode to start..."
    sleep 10

    echo ">>> Waiting for NameNode to leave safe mode..."
    for i in $(seq 1 30); do
      SAFEMODE=$(hdfs dfsadmin -safemode get 2>/dev/null || echo "unknown")
      if echo "$SAFEMODE" | grep -q "OFF"; then
        echo ">>> NameNode left safe mode after ~$((10 + i * 2))s"
        break
      fi
      if [ "$i" -eq 30 ]; then
        echo ">>> Safe mode timeout — forcing leave..."
        hdfs dfsadmin -safemode leave || true
      fi
      sleep 2
    done

    echo ">>> Creating Datalake directories in HDFS..."
    hdfs dfs -mkdir -p /hive/warehouse/datalake/raw
    hdfs dfs -mkdir -p /hive/warehouse/datalake/bronze
    hdfs dfs -mkdir -p /hive/warehouse/datalake/silver
    hdfs dfs -mkdir -p /hive/warehouse/datalake/gold
    hdfs dfs -mkdir -p /hive/warehouse/tmp
    hdfs dfs -mkdir -p /hive/warehouse/staging
    hdfs dfs -mkdir -p /user/spark/eventLog
    hdfs dfs -mkdir -p /user/spark/checkpoint
    hdfs dfs -mkdir -p /tmp
    hdfs dfs -chmod -R 777 /hive
    hdfs dfs -chmod -R 777 /user
    hdfs dfs -chmod -R 777 /tmp
    echo ">>> Datalake structure created in HDFS"

    # Upload CSV data to RAW if mounted
    if [ -d /mnt/csv-data ]; then
      echo ">>> Uploading CSV files to HDFS RAW layer..."
      hdfs dfs -put -f /mnt/csv-data/*.csv /hive/warehouse/datalake/raw/ 2>/dev/null || true
      echo ">>> RAW layer contents:"
      hdfs dfs -ls /hive/warehouse/datalake/raw/
    fi

    wait
    ;;
  datanode)
    echo ">>> Starting DataNode..."
    hdfs datanode &
    wait
    ;;
  *)
    echo ">>> Unknown role for hostname: $HOSTNAME"
    echo ">>> Starting NameNode by default..."
    hdfs namenode &
    wait
    ;;
esac
