#!/bin/bash
# ═══════════════════════════════════════════════════════════════
# Entrypoint — Spark Medallion K8s Driver
#
# 1. Configura variables de entorno dinámicas
# 2. Ejecuta pre-flight checks si existen
# 3. Delega a spark-submit o comando arbitrario
# ═══════════════════════════════════════════════════════════════
set -euo pipefail

echo "════════════════════════════════════════════════════"
echo "  Spark Medallion Pipeline — K8s Driver"
echo "  Spark: ${SPARK_VERSION:-3.3.1} | Java: $(java -version 2>&1 | head -1)"
echo "  Timestamp: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "════════════════════════════════════════════════════"

# ── JVM overrides para JDK 11+ ──
export SPARK_SUBMIT_OPTS="${SPARK_SUBMIT_OPTS:-} \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED"

# ── Pre-flight (si existe) ──
if [ -f "/opt/spark/scripts/preflight.sh" ]; then
  echo "[entrypoint] Running pre-flight checks..."
  bash /opt/spark/scripts/preflight.sh
fi

# ── Delegate to command ──
if [ "$#" -gt 0 ]; then
  exec "$@"
else
  echo "[entrypoint] No command specified. Use spark-submit or provide a command."
  exit 1
fi
