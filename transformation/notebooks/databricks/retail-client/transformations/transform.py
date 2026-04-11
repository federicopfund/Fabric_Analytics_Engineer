# Databricks notebook source
pip install adlfs

# COMMAND ----------


import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DecimalType
import pandas
import pandas as pd

# COMMAND ----------

# Configuración del acceso a la storage account donde se encuentran los archivos CSV

storage_account_name = 'storagebasedatos2510'
storage_account_access_key = 'A2unit3rNXeeU445/ZoKWxti8RhVEH+RiRk4AXfiJHwHrbO1rGB4U0eWpedsV7mPyyIZqHvEXE2f+AStgBgCZw=='
spark.conf.set('fs.azure.account.key.' + storage_account_name + '.blob.core.windows.net', storage_account_access_key)
datalake_container = 'output'
datalake_storage_name = 'storagedatalake2510'
datalake_account_access_key = '2P0PSy5tfLMxuSwmFGk5ibSVXvRVaK7gWyn2bxD+fABrMfwlRejLOvxq2rpGtpZnPnSzQiC5mlHH+AStWVsSjA=='

# COMMAND ----------

dbutils.widgets.text('parametro_direcciones','dboCategoria.csv,dboFactMine.csv,dboMine.csv')
parametro_direcciones2 = dbutils.widgets.get('parametro_direcciones')

# COMMAND ----------

blob_container = 'csv'
direcciones = parametro_direcciones2.split(',')
dataframes = []

# COMMAND ----------

for i in range(0,len(direcciones)):
    if (direcciones[i] == 'dbolanding_tables.csv' or direcciones[i] == 'dboSucursales.csv'):
        continue
    else:
        filePath = "wasbs://" + blob_container + "@" + storage_account_name + f".blob.core.windows.net/{direcciones[i]}"
        dataframes.append(spark.read.format("csv").load(filePath, inferSchema = True, header = True, encoding="utf-8"))

# COMMAND ----------

# Transformación 1
transformaciones = []
categoriaTransformada = dataframes[0].withColumnRenamed('Categoria','Nombre_Categoria')
transformaciones.append(categoriaTransformada)

# COMMAND ----------

# TRANSFORMACION 2: En la tabla “FactMine” obtener el total de la columna: TotalOreMined.

sumTotalOreMined = dataframes[1].agg(F.round(F.sum("TotalOreMined"),4).alias("Suma_TotalOreMined"))
transformaciones.append(sumTotalOreMined)

# COMMAND ----------

# Transformacion 3

selectedColumns = dataframes[2].select("Country","FirstName","LastName","Age")
transformaciones.append(selectedColumns)

# COMMAND ----------

# Transformacion 4

sumTotalWastedByCountry = dataframes[2].groupBy("Country").agg(F.round(F.sum("TotalWasted"),4).alias("Suma_TotalWasted"))
transformaciones.append(sumTotalWastedByCountry)

# COMMAND ----------

# Transformacion 5

productCount = dataframes[3].agg(F.count("Cod_Producto").alias("Cantidad_CodProducto"))
productCount = productCount.withColumn("Cantidad_CodProducto" ,F.col("Cantidad_CodProducto").cast(IntegerType()))
transformaciones.append(productCount)

# COMMAND ----------

# Transformacion 6

tableSortedByDescCode = dataframes[3].sort(F.col("Cod_Producto").desc())
transformaciones.append(tableSortedByDescCode)

# COMMAND ----------

# Transformacion 7

subcategoriaFiltered = dataframes[4].filter(dataframes[4]['Cod_Categoria'] == 3)
transformaciones.append(subcategoriaFiltered)

# COMMAND ----------

# Transformacion 8

ventasWithNetIncome = dataframes[5].withColumn("Ingresos_Netos", F.round(F.col("Cantidad")*F.col("PrecioUnitario")-F.col("CostoUnitario"),4).cast(DecimalType(10,4)))
transformaciones.append(ventasWithNetIncome)

# COMMAND ----------

# Transformacion 9

ingresosPorCodProducto = ventasWithNetIncome.groupBy(F.col("Cod_Producto")).agg(F.round(F.avg("Ingresos_Netos"),4).alias("Ingreso_Neto_Promedio"), F.round(F.sum("Ingresos_Netos"),4).alias("Suma_Ingresos_Netos"))
transformaciones.append(ingresosPorCodProducto)

# COMMAND ----------

# Carga de las tablas transformadas en formato CSV en el Azure Data Lake Storage
    
for i in range(len(transformaciones)):
    transformaciones[i].toPandas().to_csv(f'abfs://{datalake_container}@{datalake_storage_name}.dfs.core.windows.net/transformacion{i+1}.csv',storage_options = {'account_key': datalake_account_access_key} ,index=False)


# COMMAND ----------

print("hola")

# COMMAND ----------

#hola databricks
