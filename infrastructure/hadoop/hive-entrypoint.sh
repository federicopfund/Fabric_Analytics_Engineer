#!/bin/bash
# ============================================================
# Hive Metastore — Custom Entrypoint
# Maneja re-inicializaciones sin fallar por "Table already exists"
# ============================================================

export HIVE_HOME=/opt/hive
export PATH=$HIVE_HOME/bin:$PATH

echo ">>> Inicializando Hive Metastore schema..."

# Intentar initSchema; si las tablas ya existen, validar en su lugar
if schematool -initSchema -dbType mysql 2>&1; then
  echo ">>> Schema inicializado correctamente"
else
  echo ">>> Schema ya existe, verificando integridad..."
  schematool -validate -dbType mysql 2>&1 || {
    echo ">>> Intentando upgrade del schema..."
    schematool -upgradeSchema -dbType mysql 2>&1 || true
  }
fi

echo ">>> Iniciando Hive Metastore service..."
exec hive --service metastore
