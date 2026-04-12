# IBM Cloud - Medallion Pipeline

Pipeline de transformación de datos ejecutado sobre **IBM Cloud Object Storage** con **PySpark**, siguiendo la arquitectura medallion (RAW → Bronze → Silver → Gold).

## Arquitectura

```
┌──────────────────────────────────────────────────────────────────────┐
│  IBM Cloud Object Storage (us-south)                                │
│                                                                      │
│  ┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐ │
│  │ datalake-raw     │    │ datalake-bronze   │    │ datalake-silver  │ │
│  │ (CSV)            │───▶│ (Parquet)         │───▶│ (Parquet)        │ │
│  │ 7 archivos       │    │ 7 tablas          │    │ 8 tablas         │ │
│  └─────────────────┘    └──────────────────┘    └────────┬────────┘ │
│                                                           │          │
│                          ┌──────────────────┐             │          │
│                          │ datalake-gold     │◀────────────┘          │
│                          │ (Parquet/Delta)   │                       │
│                          │ 7 tablas          │──────▶ IBM Db2        │
│                          └──────────────────┘       (BI export)     │
└──────────────────────────────────────────────────────────────────────┘
```

## Prerequisitos

```bash
# 1. IBM Cloud CLI + plugins
ibmcloud plugin install cloud-object-storage container-service vpc-infrastructure

# 2. Login y target
ibmcloud login
ibmcloud target -r us-south -g Default

# 3. Configurar COS CRN
ibmcloud cos config crn --crn $(ibmcloud resource service-instance CloudObjectStorage --output json | grep -o '"crn": "[^"]*"' | head -1 | cut -d'"' -f4) --force

# 4. Instalar dependencias Python
cd transformation/notebooks/ibm-cloud
make install
```

## Comandos de ejecución

### Orquestador (ejecuta notebooks en secuencia)

```bash
cd transformation/notebooks/ibm-cloud

# Pipeline completo: Bronze → Silver → Gold
make all
# equivalente a: python orchestrator.py --layer all

# Solo una capa específica
make bronze    # RAW CSV → Bronze Parquet
make silver    # Bronze → Silver (business logic)
make gold      # Silver → Gold (star schema) + export a Db2

# Validar sin ejecutar
make dry-run

# Ver estado de la última ejecución
make status
```

### Ejecución directa con Python

```bash
python orchestrator.py --layer all        # Pipeline completo
python orchestrator.py --layer bronze     # Solo bronze
python orchestrator.py --layer silver     # Solo silver
python orchestrator.py --layer gold       # Solo gold
python orchestrator.py --dry-run          # Validación
python orchestrator.py --status           # Último status
```

## Carga de datos a COS

### Subir CSV fuente al bucket RAW

```bash
make upload-raw

# O manualmente:
ibmcloud cos object-put \
  --bucket datalake-raw-us-south \
  --key "retail/VentasInternet.csv" \
  --body ./src/main/resources/csv/VentasInternet.csv \
  --content-type "text/csv"
```

### Listar contenido de los buckets

```bash
make list-buckets

# Bucket individual:
ibmcloud cos objects --bucket datalake-bronze-us-south
```

## Estructura de archivos

```
transformation/notebooks/ibm-cloud/
├── config.py                  # Configuración compartida (COS, Db2, SparkSession)
├── orchestrator.py            # Orquestador del pipeline
├── Makefile                   # Atajos de comandos
├── README.md                  # Este archivo
├── 01_bronze_layer.ipynb      # RAW CSV → Bronze Parquet
├── 02_silver_layer.ipynb      # Bronze → Silver (transforms)
├── 03_gold_layer.ipynb        # Silver → Gold (star schema)
├── test_db2_spark.ipynb       # Test de conectividad
└── pipeline_status.json       # Estado de la última ejecución (auto-generado)
```

## Tablas por capa

### Bronze (7 tablas)
| Tabla | Fuente CSV | Keys de deduplicación |
|---|---|---|
| categoria | Categoria.csv | Cod_Categoria |
| subcategoria | Subcategoria.csv | Cod_SubCategoria |
| producto | Producto.csv | Cod_Producto |
| ventasinternet | VentasInternet.csv | NumeroOrden, Cod_Producto |
| sucursales | Sucursales.csv | Cod_Sucursal |
| factmine | FactMine.csv | TruckID, ProjectID, Date |
| mine | Mine.csv | TruckID, ProjectID, OperatorID, Date |

### Silver (8 tablas)
| Tabla | Lógica de negocio |
|---|---|
| catalogo_productos | Join producto → subcategoria → categoria |
| ventas_enriquecidas | Métricas financieras: Ingreso, Margen, Ganancia Neta |
| resumen_ventas_mensuales | Agregación mensual por categoría |
| rentabilidad_producto | Revenue, margen promedio por producto |
| segmentacion_clientes | Segmentación RFM (Recency, Frequency, Monetary) |
| produccion_operador | Producción por operador minero |
| eficiencia_minera | Eficiencia (% ore vs waste) por truck/project |
| produccion_por_pais | Producción minera por país |

### Gold (7 tablas → Star Schema)
| Tabla | Tipo | Destino |
|---|---|---|
| dim_producto | Dimensión | COS + Db2 |
| dim_cliente | Dimensión | COS + Db2 |
| dim_operador | Dimensión | COS |
| fact_ventas | Fact | COS + Db2 |
| fact_produccion_minera | Fact | COS |
| kpi_ventas_mensuales | KPI | COS + Db2 |
| kpi_mineria | KPI | COS |

## Servicios IBM Cloud utilizados

| Servicio | Nombre | Plan | Uso |
|---|---|---|---|
| Cloud Object Storage | CloudObjectStorage | Lite | Data Lake (4 buckets) |
| Db2 on Cloud | retail-db2 | Free | Tablas Gold para BI |
| DataStage | etl-datastage | Lite | Orquestación avanzada |
| Continuous Delivery | spark-cicd | Lite | CI/CD con Tekton |

## Configuración de credenciales

Las credenciales se leen del entorno. Para producción:

```bash
export COS_ACCESS_KEY="your-access-key"
export COS_SECRET_KEY="your-secret-key"
export DB2_HOST="your-db2-host"
export DB2_PORT="30376"
export DB2_USER="your-user"
export DB2_PASS="your-password"
```

## Integración CI/CD

El pipeline se puede ejecutar automáticamente desde Tekton:

```yaml
# Ver: infrastructure/ibm-cloud/tekton/pipeline.yaml
# Trigger: push a main → ejecuta tests + deploy + run pipeline
```
