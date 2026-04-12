# Lakehouse — Hadoop + Hive + Spark + Delta Lake

Implementación end-to-end de un **Data Lakehouse** sobre HDFS con Apache Hive como metastore, Apache Spark como motor de procesamiento, y Delta Lake como formato de tabla analítica.

---

## Tabla de Contenidos

- [Arquitectura](#arquitectura)
- [Componentes del Ecosistema](#componentes-del-ecosistema)
- [Nivel 1 — Instalación Inicial](#nivel-1--instalación-inicial)
- [Nivel 2 — Configuración del Datalake](#nivel-2--configuración-del-datalake)
- [Nivel 3 — Pipeline ETL (Bronze → Silver → Gold)](#nivel-3--pipeline-etl-bronze--silver--gold)
- [Nivel 4 — Hive Metastore & Consultas SQL](#nivel-4--hive-metastore--consultas-sql)
- [Nivel 5 — Producción y Monitoreo](#nivel-5--producción-y-monitoreo)
- [Estructura de Archivos](#estructura-de-archivos)
- [Troubleshooting](#troubleshooting)

---

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA LAKEHOUSE                               │
│                                                                     │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐       │
│  │   RAW    │──▶│  BRONZE  │──▶│  SILVER  │──▶│   GOLD   │       │
│  │  (CSV)   │   │ (Parquet)│   │ (Parquet)│   │ (Delta)  │       │
│  └──────────┘   └──────────┘   └──────────┘   └──────────┘       │
│       ▲                                             │              │
│       │              HDFS (Storage Layer)            │              │
│  ┌────┴─────┐                                 ┌─────▼──────┐      │
│  │  CSV     │                                 │   Hive     │      │
│  │  Files   │                                 │  Catalog   │      │
│  └──────────┘                                 └─────┬──────┘      │
│                                                     │              │
│                    ┌────────────────────────────┐    │              │
│                    │    Apache Spark 3.3.1      │◀───┘              │
│                    │  (Processing Engine)       │                   │
│                    └────────────────────────────┘                   │
│                                                                     │
│  Infrastructure: Docker │ NameNode │ DataNode │ YARN │ Hive        │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Componentes del Ecosistema

| Componente | Versión | Puerto | Función |
|---|---|---|---|
| **Hadoop HDFS** | 3.3.4 | 9000 (RPC), 9870 (UI) | Storage distribuido |
| **YARN** | 3.3.4 | 8088 (UI) | Resource management |
| **Hive Metastore** | 3.1.3 | 9083 (Thrift) | Catálogo de tablas |
| **HiveServer2** | 3.1.3 | 10000 (JDBC), 10002 (UI) | Interfaz SQL |
| **PostgreSQL** | 15 | 5433 | Backend del metastore |
| **Apache Spark** | 3.3.1 | — | Motor de procesamiento |
| **Delta Lake** | 2.2.0 | — | Formato ACID en Gold |

---

## Nivel 1 — Instalación Inicial

### Prerrequisitos

- Docker & Docker Compose
- Java 11+ (para ejecución local de Spark)
- sbt (para compilar el ETL Scala)

### 1.1 Clonar y posicionarse

```bash
cd infrastructure/hadoop
```

### 1.2 Construir las imágenes Docker

```bash
docker-compose build
```

### 1.3 Levantar el ecosistema completo

```bash
docker-compose up -d
```

### 1.4 Verificar servicios

```bash
# Estado de los contenedores
docker-compose ps

# Reporte de HDFS
docker exec -it namenode hdfs dfsadmin -report

# Safe mode status
docker exec -it namenode hdfs dfsadmin -safemode get
```

### 1.5 Acceder a las interfaces web

| Servicio | URL |
|---|---|
| NameNode | http://localhost:9870 |
| DataNode | http://localhost:9864 |
| YARN ResourceManager | http://localhost:8088 |
| HiveServer2 | http://localhost:10002 |

---

## Nivel 2 — Configuración del Datalake

### 2.1 Estructura del Datalake en HDFS

El `entrypoint.sh` crea automáticamente la estructura:

```
/hive/warehouse/datalake/
├── raw/          ← Datos crudos (CSV originales)
├── bronze/       ← Datos limpios (Parquet)
├── silver/       ← Lógica de negocio (Parquet)
└── gold/         ← Modelos BI (Delta Lake)
```

### 2.2 Verificar la estructura

```bash
docker exec -it namenode hdfs dfs -ls -R /hive/warehouse/datalake/
```

### 2.3 Carga manual de datos (si no se auto-cargaron)

```bash
# Los CSV se montan en /mnt/csv-data dentro del NameNode
docker exec -it namenode hdfs dfs -put /mnt/csv-data/*.csv /hive/warehouse/datalake/raw/

# Verificar
docker exec -it namenode hdfs dfs -ls /hive/warehouse/datalake/raw/
```

### 2.4 Archivos de datos esperados

| Archivo | Dominio | Descripción |
|---|---|---|
| `Categoria.csv` | Retail | Categorías de productos |
| `Subcategoria.csv` | Retail | Subcategorías |
| `Producto.csv` | Retail | Catálogo de productos |
| `VentasInternet.csv` | Retail | Transacciones de venta |
| `Sucursales.csv` | Retail | Ubicaciones de sucursales |
| `FactMine.csv` | Mining | Hechos de producción minera |
| `Mine.csv` | Mining | Operaciones mineras detalladas |

### 2.5 Configuración Hadoop desde Scala (Workflow)

El `Workflow.setupHadoopEnvironment()` configura programáticamente:

```scala
// Desde el código Scala (main.scala)
val hadoopConfig = Workflow.setupHadoopEnvironment("hdfs://namenode:9000")

// Esto ejecuta:
// 1. buildHadoopConfiguration() — core-site, hdfs-site, hive-site
// 2. createDatalakeStructure()  — crea /raw, /bronze, /silver, /gold
// 3. validateDatalake()         — verifica integridad
```

---

## Nivel 3 — Pipeline ETL (Bronze → Silver → Gold)

### 3.1 Compilar el proyecto Scala

```bash
cd transformation/spark-jobs/pipelines/batch-etl-scala
sbt clean assembly
```

### 3.2 Ejecutar en modo LOCAL (sin HDFS)

```bash
sbt "runMain main.MainETL"
```

Automáticamente detecta que HDFS no está disponible y usa el filesystem local:
- Crea `./datalake/raw/`, `./datalake/bronze/`, `./datalake/silver/`, `./datalake/gold/`
- Copia los CSV de `src/main/resources/csv/` a `raw/`

### 3.3 Ejecutar en modo HDFS (con el ecosistema Docker)

```bash
export HDFS_URI="hdfs://localhost:9000"
export HIVE_METASTORE_URI="thrift://localhost:9083"
export CSV_PATH="$(pwd)/src/main/resources/csv"

sbt "runMain main.MainETL"
```

### 3.4 Ejecutar con spark-submit

```bash
spark-submit \
  --class main.MainETL \
  --master local[*] \
  --conf spark.hadoop.fs.defaultFS=hdfs://localhost:9000 \
  --conf spark.sql.warehouse.dir=hdfs://localhost:9000/hive/warehouse/ \
  --packages io.delta:delta-core_2.12:2.2.0 \
  target/scala-2.12/batch-etl-scala-assembly-1.0.0.jar
```

### 3.5 Pipeline Stages

#### STAGE 1: BRONZE — Data Cleansing
- Lee CSV con schemas explícitos (tipado fuerte)
- Elimina filas con claves nulas
- Deduplicación por claves naturales
- Agrega metadatos de auditoría (`_bronze_ingested_at`, `_bronze_source_file`)
- Escribe en formato **Parquet**

#### STAGE 2: SILVER — Business Logic
- **Retail**: catalogo_productos, ventas_enriquecidas, resumen_ventas_mensuales, rentabilidad_producto, segmentacion_clientes (RFM)
- **Mining**: produccion_operador, eficiencia_minera, produccion_por_pais
- JOINs complejos, cálculos financieros, clasificaciones

#### STAGE 3: GOLD — BI & Analytics Models
- **Star Schema**: dim_producto, dim_cliente, fact_ventas, kpi_ventas_mensuales
- **Mining KPIs**: dim_operador, fact_produccion_minera, kpi_mineria
- Formato **Delta Lake** con soporte ACID
- Variaciones MoM, acumulados YTD, scores de segmentación

#### STAGE 4: HIVE — Catalog Registration (cuando HDFS está activo)
- Registra tablas Gold como tablas Delta en Hive
- Registra tablas Silver como tablas Parquet externas
- Database: `lakehouse`

---

## Nivel 4 — Hive Metastore & Consultas SQL

### 4.1 Conectar con Beeline

```bash
docker exec -it hiveserver2 beeline -u jdbc:hive2://localhost:10000
```

### 4.2 Consultas básicas

```sql
-- Ver bases de datos
SHOW DATABASES;

-- Usar la base del lakehouse
USE lakehouse;

-- Ver tablas registradas
SHOW TABLES;

-- Consultar dimensión de productos
SELECT * FROM dim_producto LIMIT 10;

-- KPIs de ventas mensuales
SELECT
  periodo,
  categoria,
  ingreso_bruto,
  margen_bruto,
  variacion_mom_pct
FROM kpi_ventas_mensuales
ORDER BY periodo DESC
LIMIT 20;

-- Segmentación de clientes
SELECT
  segmento,
  COUNT(*) AS total_clientes,
  ROUND(AVG(valor_monetario), 2) AS avg_monetary,
  ROUND(AVG(frecuencia_compras), 1) AS avg_frecuencia
FROM dim_cliente
GROUP BY segmento
ORDER BY avg_monetary DESC;

-- KPIs de minería
SELECT
  pais,
  total_mineral,
  produccion_neta,
  tasa_desperdicio_pct,
  evaluacion_operativa
FROM kpi_mineria
ORDER BY produccion_neta DESC;
```

### 4.3 Tablas disponibles en Hive

#### Gold Layer (Delta Lake)

| Tabla | Descripción |
|---|---|
| `dim_producto` | Dimensión producto con clasificación rentabilidad/rotación |
| `dim_cliente` | Dimensión cliente con segmentación RFM y LTV |
| `fact_ventas` | Hechos de venta con métricas financieras |
| `kpi_ventas_mensuales` | KPIs ejecutivos con MoM y YTD |
| `dim_operador` | Dimensión operador minero con ranking |
| `fact_produccion_minera` | Hechos de producción con eficiencia |
| `kpi_mineria` | KPIs ejecutivos de operación minera |

#### Silver Layer (Parquet)

| Tabla | Descripción |
|---|---|
| `silver_catalogo_productos` | Catálogo con jerarquía categoría/subcategoría |
| `silver_ventas_enriquecidas` | Ventas con margen, tipo envío, promoción |
| `silver_resumen_ventas_mensuales` | Agregación mensual por categoría |
| `silver_rentabilidad_producto` | Revenue, margen y ranking por producto |
| `silver_segmentacion_clientes` | Segmentación RFM de clientes |
| `silver_produccion_operador` | Producción y eficiencia por operador |
| `silver_eficiencia_minera` | Eficiencia por truck/proyecto |
| `silver_produccion_por_pais` | Producción agregada por país |

---

## Nivel 5 — Producción y Monitoreo

### 5.1 Script de inicio completo (E2E)

```bash
cd infrastructure/hadoop
bash lakehouse-start.sh
```

Este script:
1. Construye y levanta toda la infraestructura Docker
2. Espera a que NameNode y Hive Metastore estén listos
3. Verifica la estructura del Datalake en HDFS
4. Compila el ETL Scala
5. Ejecuta el pipeline completo
6. Muestra resultados y URLs de acceso

### 5.2 Dashboard de estado

```bash
bash dashboard.sh
```

### 5.3 Variables de entorno

| Variable | Default | Descripción |
|---|---|---|
| `HDFS_URI` | `hdfs://namenode:9000` | URI del NameNode |
| `HIVE_METASTORE_URI` | `thrift://localhost:9083` | URI del Hive Metastore |
| `CSV_PATH` | `./src/main/resources/csv` | Path local a los CSV fuente |

### 5.4 Configuración del Workflow (Scala)

```scala
// El Workflow configura Hadoop programáticamente:
val config = HdfsManager.HadoopConfig(
  hdfsUri          = "hdfs://namenode:9000",
  user             = "fede",
  replication      = 1,          // 3 en producción
  blockSize        = 134217728L, // 128MB
  hiveWarehouse    = "/hive/warehouse",
  hiveMetastoreUris = "thrift://hive-metastore:9083"
)

HdfsManager.buildHadoopConfiguration(config)
```

### 5.5 Mejoras para producción

- Cambiar `dfs.replication` a **3**
- Cambiar permisos de `770` a reglas específicas por rol
- Configurar Kerberos para autenticación
- Usar un PostgreSQL externo para el Hive Metastore
- Configurar alta disponibilidad del NameNode (QJM)
- Agregar Ranger para control de acceso a tablas
- Configurar Spark event log en HDFS
- Implementar scheduling con Apache Airflow

### 5.6 Ejecución en YARN (cluster mode)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 4 \
  --executor-cores 2 \
  --executor-memory 2g \
  --driver-memory 1g \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  --conf spark.sql.warehouse.dir=hdfs://namenode:9000/hive/warehouse/ \
  --conf spark.sql.hive.metastore.version=3.1.3 \
  --conf spark.sql.hive.metastore.jars=builtin \
  --packages io.delta:delta-core_2.12:2.2.0 \
  --class main.MainETL \
  hdfs://namenode:9000/user/spark/jars/batch-etl-assembly.jar
```

---

## Estructura de Archivos

```
infrastructure/hadoop/
├── docker-compose.yaml        ← Ecosistema completo (HDFS + YARN + Hive)
├── Dockerfile                 ← Imagen Hadoop 3.3.4
├── entrypoint.sh              ← Script de inicio por rol (namenode/datanode/...)
├── lakehouse-start.sh         ← Script E2E (infra + ETL)
├── dashboard.sh               ← Status dashboard
├── comandos.sh                ← Referencia de comandos
└── conf/
    ├── core-site.xml          ← Configuración core de Hadoop
    ├── hdfs-site.xml          ← Configuración HDFS
    ├── hive-site.xml          ← Configuración Hive Metastore
    ├── mapred-site.xml        ← Configuración MapReduce
    └── yarn-site.xml          ← Configuración YARN

transformation/spark-jobs/pipelines/batch-etl-scala/
├── build.sbt                  ← Dependencias (Spark + Hadoop + Hive + Delta)
└── src/main/scala/
    ├── main.scala             ← Entry point (detecta HDFS/local)
    ├── StoreHDFS/
    │   └── hdfs.scala         ← HdfsManager (config + CRUD + validación)
    ├── workflow/
    │   └── Workflow.scala     ← Orquestador (setup + pipeline + Hive catalog)
    ├── common/
    │   ├── sparksession.scala ← SparkSession con Hive support
    │   ├── DataLakeIO.scala   ← Read/Write (CSV, Parquet, Delta)
    │   └── Schemas.scala      ← Schemas tipados por dominio
    ├── bronze/
    │   └── BronzeLayer.scala  ← Limpieza y deduplicación
    ├── silver/
    │   └── SilverLayer.scala  ← Lógica de negocio y JOINs
    └── gold/
        └── GoldLayer.scala    ← Star Schema y KPIs (Delta Lake)
```

---

## Troubleshooting

### HDFS en Safe Mode
```bash
docker exec -it namenode hdfs dfsadmin -safemode leave
```

### Hive Metastore no conecta
```bash
# Verificar que PostgreSQL esté listo
docker logs hive-metastore-db

# Verificar que el Metastore puede conectar a HDFS
docker logs hive-metastore
```

### Spark no encuentra tablas Hive
```bash
# Verificar que hive.metastore.uris esté configurado
# En SparkSession debe tener .enableHiveSupport()
# Verificar conectividad: thrift://localhost:9083
```

### Permisos HDFS
```bash
docker exec -it namenode hdfs dfs -chmod -R 777 /hive/warehouse/
```

### Limpiar y reiniciar todo
```bash
cd infrastructure/hadoop
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d
```
