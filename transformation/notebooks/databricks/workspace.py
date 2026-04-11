# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Sample notebook

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Configurar la autenticación

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Example usage of existing code

# COMMAND ----------

# export DATABRICKS_HOST="https://adb-805426422713868.8.azuredatabricks.net"
# export DATABRICKS_TOKEN="dapic0ed08e4633f283acef4bdeb28a16de6-3"


# COMMAND ----------
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.clusters.api import ClusterApi
from databricks_cli.dbfs.dbfs_path import DbfsPath
from databricks_cli.dbfs.api import DbfsApi
import os
# COMMAND ----------

api_client = ApiClient(
  host  = os.getenv('DATABRICKS_HOST'),
  token = os.getenv('DATABRICKS_TOKEN')
)
# COMMAND ----------
# MAGIC
# MAGIC ## inicializar una instancia de Clusters API 2.0, agregue el siguiente código:
clusters_api = ClusterApi(api_client)
clusters_list = clusters_api.list_clusters()
# COMMAND ----------

print("Cluster name, cluster ID")

for cluster in clusters_list['clusters']:
  print(f"{cluster['cluster_name']}, {cluster['cluster_id']}")

# COMMAND ----------


# MAGIC
# MAGIC ## Descargar un archivo desde una ruta DBFS

#spark.read.format("csv").load(f"file:{os.getcwd()}/my_data.csv")

dbfs_source_file_path      = 'dbfs:/tmp'
local_file_download_path   = './'

dbfs_api  = DbfsApi(api_client)
dbfs_path = DbfsPath(dbfs_source_file_path)

# Download the workspace file locally.
dbfs_api.get_file(
  dbfs_path,
  local_file_download_path,
  overwrite = True
)

# Print the downloaded file's contents.
print(open(local_file_download_path, 'r').read())
# COMMAND ----------