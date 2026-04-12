#!/bin/bash
# ============================================================
# Lakehouse E2E — Start full environment and run ETL pipeline
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
ETL_DIR="$PROJECT_ROOT/transformation/spark-jobs/pipelines/batch-etl-scala"

echo "╔══════════════════════════════════════════════════╗"
echo "║     LAKEHOUSE ENVIRONMENT — E2E SETUP            ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# ── STEP 1: Start Hadoop + Hive infrastructure ────────────
echo "┌──────────────────────────────────────────┐"
echo "│  STEP 1: Start Infrastructure            │"
echo "└──────────────────────────────────────────┘"
cd "$SCRIPT_DIR"
docker-compose build --no-cache
docker-compose up -d

echo ">>> Waiting for NameNode to be ready..."
until docker exec namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; do
  echo "  ... NameNode still starting"
  sleep 5
done
echo "✔ NameNode ready"

echo ">>> Waiting for Hive Metastore..."
until docker exec hive-metastore bash -c "echo > /dev/tcp/localhost/9083" 2>/dev/null; do
  echo "  ... Hive Metastore starting"
  sleep 5
done
echo "✔ Hive Metastore ready"

# ── STEP 2: Verify HDFS Datalake Structure ────────────────
echo ""
echo "┌──────────────────────────────────────────┐"
echo "│  STEP 2: Verify Datalake Structure       │"
echo "└──────────────────────────────────────────┘"
docker exec namenode hdfs dfs -ls -R /hive/warehouse/datalake/ || true
echo ""
echo ">>> RAW layer contents:"
docker exec namenode hdfs dfs -ls /hive/warehouse/datalake/raw/ || echo "  (empty — will be populated by ETL)"

# ── STEP 3: Build ETL Assembly ────────────────────────────
echo ""
echo "┌──────────────────────────────────────────┐"
echo "│  STEP 3: Build Spark ETL Assembly        │"
echo "└──────────────────────────────────────────┘"
cd "$ETL_DIR"
if command -v sbt &>/dev/null; then
  sbt clean assembly
  echo "✔ ETL JAR built"
else
  echo "⚠ sbt not found — skip build (use pre-built JAR)"
fi

# ── STEP 4: Run ETL Pipeline ─────────────────────────────
echo ""
echo "┌──────────────────────────────────────────┐"
echo "│  STEP 4: Run ETL Pipeline                │"
echo "└──────────────────────────────────────────┘"

JAR_FILE=$(find "$ETL_DIR/target/scala-2.12/" -name "*.jar" -type f 2>/dev/null | head -1)

if [ -n "$JAR_FILE" ]; then
  export HDFS_URI="hdfs://localhost:9000"
  export HIVE_METASTORE_URI="thrift://localhost:9083"
  export CSV_PATH="$ETL_DIR/src/main/resources/csv"

  if command -v spark-submit &>/dev/null; then
    spark-submit \
      --class main.MainETL \
      --master local[*] \
      --conf spark.hadoop.fs.defaultFS=$HDFS_URI \
      --conf spark.sql.warehouse.dir=$HDFS_URI/hive/warehouse/ \
      --packages io.delta:delta-core_2.12:2.2.0 \
      "$JAR_FILE"
  else
    echo "⚠ spark-submit not found — run ETL via sbt:"
    echo "  cd $ETL_DIR && sbt run"
  fi
else
  echo "⚠ No JAR found — running ETL via sbt..."
  cd "$ETL_DIR"
  sbt "runMain main.MainETL" 2>&1 || echo "⚠ ETL run requires sbt"
fi

# ── STEP 5: Verify Results ───────────────────────────────
echo ""
echo "┌──────────────────────────────────────────┐"
echo "│  STEP 5: Verify Lakehouse Results        │"
echo "└──────────────────────────────────────────┘"
echo ">>> Datalake layers:"
for layer in raw bronze silver gold; do
  count=$(docker exec namenode hdfs dfs -ls /hive/warehouse/datalake/$layer/ 2>/dev/null | wc -l || echo "0")
  echo "  ├── $layer: $count entries"
done

echo ""
echo ">>> Hive tables:"
docker exec hiveserver2 beeline -u jdbc:hive2://localhost:10000 \
  -e "SHOW DATABASES; USE lakehouse; SHOW TABLES;" 2>/dev/null || \
  echo "  (HiveServer2 may still be initializing)"

# ── SUMMARY ──────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║  LAKEHOUSE ENVIRONMENT READY                     ║"
echo "╠══════════════════════════════════════════════════╣"
echo "║  NameNode UI:     http://localhost:9870          ║"
echo "║  DataNode UI:     http://localhost:9864          ║"
echo "║  YARN UI:         http://localhost:8088          ║"
echo "║  HiveServer2 UI:  http://localhost:10002         ║"
echo "║  Hive Metastore:  thrift://localhost:9083        ║"
echo "║  HDFS:            hdfs://localhost:9000           ║"
echo "╚══════════════════════════════════════════════════╝"
