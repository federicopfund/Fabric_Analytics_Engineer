# Hadoop Lakehouse — Documentación Técnica

## Resumen

Entorno local de lakehouse basado en Docker Compose que despliega HDFS (NameNode + DataNode), Hive Metastore con MySQL como backend, y Spark para procesamiento. Diseñado para desarrollo, testing y staging del pipeline Medallion.

---

## Arquitectura Docker Compose

```mermaid
graph TB
    subgraph Docker["Docker Compose — Lakehouse"]
        subgraph HDFS["HDFS Cluster"]
            NN[NameNode<br/>:9870 UI<br/>:9000 RPC]
            DN[DataNode<br/>:9864 UI<br/>:9866 Transfer]
        end

        subgraph Hive["Hive Stack"]
            HMS[Hive Metastore<br/>:9083 Thrift]
            MYSQL[(MySQL<br/>:3306<br/>metastore DB)]
        end

        subgraph Compute["Spark Processing"]
            SPARK[Spark Driver<br/>+ Workers]
        end
    end

    NN <-->|Heartbeat + Blocks| DN
    HMS --> MYSQL
    HMS --> NN
    SPARK --> NN
    SPARK --> HMS

    subgraph Storage["Almacenamiento HDFS"]
        RAW["/datalake/raw/<br/>CSVs originales"]
        BRONZE["/datalake/bronze/<br/>Parquet validado"]
        SILVER["/datalake/silver/<br/>Parquet agregado"]
        GOLD["/datalake/gold/<br/>Delta Lake"]
        WAREHOUSE["/hive/warehouse/<br/>Tablas Hive"]
    end

    DN --> Storage
```

---

## Componentes

### NameNode

```mermaid
graph LR
    NN[NameNode] --> META["Metadata<br/>Block locations<br/>File namespace"]
    NN --> WEBUI["Web UI :9870<br/>Browse filesystem<br/>Cluster health"]
    NN --> RPC["RPC :9000<br/>hdfs://namenode:9000"]
    NN --> VOL[("Volume<br/>/hadoop/dfs/name")]
```

| Config | Valor |
|--------|-------|
| `fs.defaultFS` | `hdfs://namenode:9000` |
| Replicación | 1 (desarrollo) |
| WebHDFS | Habilitado |
| Superuser group | `hadoop` |
| Umask | `022` |
| Proxy users | `hive`, `root` (para todos los hosts) |

### DataNode

| Config | Valor |
|--------|-------|
| Directorio | `/hadoop/dfs/data` |
| Puerto UI | 9864 |
| Puerto transfer | 9866 |
| Volumen | `/hadoop/dfs/data` (persistent) |

### Hive Metastore

```mermaid
graph TB
    CLIENT[Spark / Beeline] -->|Thrift :9083| HMS[Hive Metastore]
    HMS -->|JDBC| MYSQL[(MySQL :3306<br/>database: metastore)]
    HMS -->|Warehouse| HDFS["hdfs://namenode:9000<br/>/hive/warehouse"]

    HMS --> TABLES["Tablas registradas:<br/>• Dimensiones<br/>• Hechos<br/>• Vistas"]
```

| Config | Valor |
|--------|-------|
| URI Metastore | `thrift://hive-metastore:9083` |
| Warehouse | `hdfs://namenode:9000/hive/warehouse` |
| Backend DB | `mysql://hive-metastore-db:3306/metastore` |
| Driver | `com.mysql.cj.jdbc.Driver` |

### YARN / MapReduce

| Config | Valor |
|--------|-------|
| Framework | YARN |
| ResourceManager | `localhost:8088` |
| Job History | `localhost:10020` |
| NodeManager recovery | 10 apps |

---

## Dockerfile — Imagen Hadoop

```mermaid
graph TB
    BASE["eclipse-temurin:11-jre-focal"] --> DEPS["apt install:<br/>curl, ssh, rsync,<br/>net-tools"]
    DEPS --> HADOOP["Download:<br/>Apache Hadoop 3.3.4<br/>(archive.apache.org)"]
    HADOOP --> SSH["SSH Config:<br/>Passwordless auth<br/>for start-dfs.sh"]
    SSH --> FORMAT["hdfs namenode<br/>-format -force"]
    FORMAT --> VOLUMES["Volumes:<br/>/hadoop/dfs/name<br/>/hadoop/dfs/data"]
    VOLUMES --> IMAGE["Imagen Final<br/>Hadoop 3.3.4 + Java 11"]
```

---

## Archivos de Configuración

### `conf/core-site.xml`

```xml
<!-- Filesystem default -->
fs.defaultFS = hdfs://namenode:9000
<!-- Umask -->
fs.permissions.umask-mode = 022
<!-- Proxy users para Hive -->
hadoop.proxyuser.hive.hosts = *
hadoop.proxyuser.hive.groups = *
```

### `conf/hdfs-site.xml`

```xml
dfs.replication = 1
dfs.namenode.name.dir = file:///hadoop/dfs/name
dfs.datanode.data.dir = file:///hadoop/dfs/data
dfs.webhdfs.enabled = true
dfs.permissions.superusergroup = hadoop
```

### `conf/hive-site.xml`

```xml
hive.metastore.uris = thrift://hive-metastore:9083
hive.metastore.warehouse.dir = hdfs://namenode:9000/hive/warehouse
javax.jdo.option.ConnectionURL = jdbc:mysql://hive-metastore-db:3306/metastore
javax.jdo.option.ConnectionDriverName = com.mysql.cj.jdbc.Driver
```

---

## Estructura de Datos en HDFS

```mermaid
graph TB
    ROOT["/"] --> DATALAKE["/datalake"]
    ROOT --> HIVE_DIR["/hive"]
    ROOT --> USER["/user"]

    DATALAKE --> RAW["/datalake/raw/<br/>Categoria.csv<br/>Subcategoria.csv<br/>Producto.csv<br/>Cliente.csv<br/>Orden.csv<br/>OrdenDetalle.csv<br/>FactMine.csv"]
    DATALAKE --> BRONZE["/datalake/bronze/<br/>Categoria/<br/>Producto/<br/>Cliente/<br/>..."]
    DATALAKE --> SILVER["/datalake/silver/<br/>dim_categoria/<br/>dim_producto/<br/>fact_ordenes/<br/>..."]
    DATALAKE --> GOLD["/datalake/gold/<br/>fact_ventas/<br/>dim_cliente/<br/>dim_producto/<br/>kpi_ventas_mensuales/"]
    DATALAKE --> CHECKPOINTS["/datalake/checkpoints/<br/>task_status.json"]

    HIVE_DIR --> WAREHOUSE["/hive/warehouse/<br/>Tablas externas"]
    USER --> HISTORY["/user/history/<br/>MapReduce logs"]
```

---

## Scripts Auxiliares

| Script | Propósito |
|--------|-----------|
| `comandos.sh` | Comandos HDFS frecuentes (ls, mkdir, put, get) |
| `dashboard.sh` | Verificar estado del cluster |
| `entrypoint.sh` | Entrypoint Docker para NameNode/DataNode |
| `hive-entrypoint.sh` | Inicializar schema de Hive Metastore |
| `lakehouse-start.sh` | Levantar stack completo y crear estructura de directorios |

---

## Puertos Expuestos

| Servicio | Puerto | Protocolo | Uso |
|----------|--------|-----------|-----|
| NameNode UI | 9870 | HTTP | Browse filesystem, cluster health |
| NameNode RPC | 9000 | TCP | HDFS client connections |
| DataNode UI | 9864 | HTTP | Block reports |
| DataNode Transfer | 9866 | TCP | Data transfer |
| Hive Metastore | 9083 | Thrift | Metadata queries |
| MySQL | 3306 | TCP | Metastore backend |
| YARN RM | 8088 | HTTP | ResourceManager UI |
| Job History | 10020 | TCP | MapReduce history |
