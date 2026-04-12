#!/bin/bash
# ============================================================
# Lakehouse Environment — Commands Reference
# ============================================================

# ── BUILD & START ──────────────────────────────────────────
docker-compose build
docker-compose up -d

# ── VERIFY SERVICES ────────────────────────────────────────
docker-compose ps
docker exec -it namenode hdfs dfsadmin -report
docker exec -it namenode hdfs dfsadmin -safemode get

# ── DATALAKE STRUCTURE ─────────────────────────────────────
docker exec -it namenode hdfs dfs -ls -R /hive/warehouse/datalake/

# ── UPLOAD RAW DATA ────────────────────────────────────────
# CSVs are auto-uploaded via entrypoint, but manual upload:
docker exec -it namenode hdfs dfs -put /mnt/csv-data/*.csv /hive/warehouse/datalake/raw/
docker exec -it namenode hdfs dfs -ls /hive/warehouse/datalake/raw/

# ── HIVE QUERIES (via Beeline) ────────────────────────────
docker exec -it hiveserver2 beeline -u jdbc:hive2://localhost:10000
# Inside beeline:
#   SHOW DATABASES;
#   CREATE DATABASE IF NOT EXISTS lakehouse;
#   USE lakehouse;
#   SHOW TABLES;

# ── SPARK SUBMIT (ETL Pipeline) ───────────────────────────
# Local mode:
# sbt assembly
# spark-submit --class main.MainETL target/scala-2.12/*.jar

# Cluster mode (via YARN):
# spark-submit \
#   --master yarn \
#   --deploy-mode client \
#   --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
#   --class main.MainETL target/scala-2.12/*.jar

# ── MONITORING ─────────────────────────────────────────────
# NameNode UI:       http://localhost:9870
# DataNode UI:       http://localhost:9864
# YARN ResourceMgr:  http://localhost:8088
# Hive Server2 UI:   http://localhost:10002

# ── CLEANUP ────────────────────────────────────────────────
docker-compose down
docker-compose down -v  # also remove volumes
