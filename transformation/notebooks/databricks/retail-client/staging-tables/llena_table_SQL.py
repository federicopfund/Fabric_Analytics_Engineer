# Databricks notebook source
# DBTITLE 1,                                                         Importamos librerias
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql import *
import pandas as pd
import random

# COMMAND ----------

# DBTITLE 1,                                               Setiar variables de Base de dato
jdbcHostname = "server--xk1rme4.database.windows.net"
jdbcPort = 1433
jdbcDatabase = "dbRetail"
jdbcUsername = "admin25"
jdbcPassword = "Siglo2510"
jdbcDriver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

jdbcUrl=f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};databaseName={jdbcDatabase};user={jdbcUsername};password={jdbcPassword}"

# COMMAND ----------

# DBTITLE 1,                                    Borrar la tabla si existe en el contexto de Databricks
# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS Categoria;
# MAGIC DROP TABLE IF EXISTS Categoria;
# MAGIC DROP TABLE IF EXISTS Categoria;

# COMMAND ----------

# DBTITLE 1,                                                   Lectura de la tabla dbo.Categoria

categoria_table = (spark.read
  .format("jdbc")
  .option("url", jdbcUrl)
  .option("dbtable", "dbo.Categoria")
  .load()
)

# COMMAND ----------

# DBTITLE 1,                                                 Seleccionamos todo de la tabla categoria
# MAGIC %sql
# MAGIC select * from Categoria

# COMMAND ----------

# DBTITLE 1,                      Guardamos la tabla Categoría, como una tabla en el entorno de Databricks

if 'categoria' not in sqlContext.tableNames('default'):
    categoria_table.write.saveAsTable('Categoria')

# COMMAND ----------

# DBTITLE 1,                                                           Visualizamos el Schema
categoria_table.printSchema()

# COMMAND ----------

display(categoria_table)

# COMMAND ----------

# DBTITLE 1,                                                        Lectura de la tabla dbo.Producto
producto_table = (spark.read
  .format("jdbc")
  .option("url", jdbcUrl)
  .option("dbtable", "dbo.Producto")
  .load())

# COMMAND ----------

# DBTITLE 1,                                     Guardamos  tabla Producto, en el entorno de Databricks
if 'producto' not in sqlContext.tableNames('default'):
    producto_table.write.saveAsTable('Producto')

# COMMAND ----------

# DBTITLE 1,                                                             Visualización de Schema
producto_table.printSchema()

# COMMAND ----------

# DBTITLE 1,                                                       Lectura de la tabla Subcategoria
subcategoria_table = (spark.read
  .format("jdbc")
  .option("url", jdbcUrl)
  .option("dbtable", "dbo.SubCategoria")
  .load())

# COMMAND ----------

# DBTITLE 1,                                                    Guardamos la tabla si no existe

if 'subcategoria' not in sqlContext.tableNames('default'):
    subcategoria_table.write.saveAsTable('Subcategoria')

# COMMAND ----------

# DBTITLE 1,                                                          Visualización  de Schema
subcategoria_table.printSchema()

# COMMAND ----------

# DBTITLE 1,                                                    Lectura de la tabla Sucursales 
sucursales_table = (spark.read
  .format("jdbc")
  .option("url", jdbcUrl)
  .option("dbtable", "dbo.Sucursales")
  .load())

# COMMAND ----------

# DBTITLE 1,                                              Guardamos la Tabla en Databricks
if 'sucursales' not in sqlContext.tableNames('default'):
    sucursales_table.write.saveAsTable('Sucursales')

# COMMAND ----------

# DBTITLE 1,                                                   Visualización del Schema
sucursales_table.printSchema()

# COMMAND ----------

# DBTITLE 1,                                             Tabla intermedia Producto_Unico
# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE Producto_Unico (
# MAGIC   Cod_Producto bigint GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   Producto VARCHAR(50),
# MAGIC   Cod_Subcategoria INT,
# MAGIC   Cod_Categoria INT
# MAGIC   );

# COMMAND ----------

# DBTITLE 1,                                            Relaciona de tabla intermedia donde vamos 
# MAGIC %sql
# MAGIC 
# MAGIC --  relacionar los productos con sus sucursales y su stock
# MAGIC CREATE TABLE Productos_Sucursales (
# MAGIC   Cod_Producto INT,
# MAGIC   Cod_Sucursal INT,
# MAGIC   Stock INT,
# MAGIC   Stock_Inicial INT
# MAGIC   );

# COMMAND ----------

# DBTITLE 1,                                                     Creamos vista de una consulta
# MAGIC %sql
# MAGIC -- Guardamos la consulta, en un view productos_distintos, para tenerla a mano y poder utilizarla más adelante
# MAGIC CREATE VIEW productos_distintos AS
# MAGIC SELECT MIN(Cod_Producto) AS Cod_Producto, Producto, MIN(Cod_SubCategoria) AS Cod_SubCategoria, COUNT(Producto) AS Stock_Inicial FROM Producto
# MAGIC GROUP BY Producto
# MAGIC ORDER BY Cod_Producto;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Guardamos la consulta, en una view productos_unicos_joined, para tenerla a mano y poder utilizarla más adelante
# MAGIC CREATE VIEW productos_unicos_joined AS
# MAGIC SELECT t1.Cod_Producto, t1.Producto, t1.Cod_SubCategoria, t2.Cod_Categoria
# MAGIC FROM productos_distintos AS t1
# MAGIC INNER JOIN Subcategoria AS t2
# MAGIC ON t1.Cod_SubCategoria = t2.Cod_SubCategoria
# MAGIC ORDER BY t1.Cod_Producto;

# COMMAND ----------

# DBTITLE 1,                                                              Rellenamos la tablas
# MAGIC %sql
# MAGIC -- Rellenamos la tabla de Producto_Unico, que ya creamos, con el contenido de la view anteior (productos_unicos_joined)
# MAGIC INSERT INTO Producto_Unico (Producto, Cod_SubCategoria, Cod_Categoria)
# MAGIC SELECT Producto, Cod_SubCategoria, Cod_Categoria FROM productos_unicos_joined;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insertamos el producto cartesiano en la tabla
# MAGIC INSERT INTO Productos_Sucursales
# MAGIC SELECT t1.Cod_Producto, t2.Cod_Sucursal, 0 AS Stock, 0 as Stock_Inicial FROM Producto_Unico AS t1
# MAGIC CROSS JOIN (SELECT * FROM Sucursales WHERE Cod_Sucursal_PK <> 0) AS t2;

# COMMAND ----------

# DBTITLE 1,                                                                   Verificación
# MAGIC %sql
# MAGIC -- Select para verificar se que insertó todo bien
# MAGIC SELECT * FROM Producto_Unico;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificamos como poblar la tabla, se realiza el producto cartesiano entre cod_producto y sucursal y se inicializa el stock en 0
# MAGIC -- Producto cartesiano: aparea cada producto con cada sucursal
# MAGIC SELECT t1.Cod_Producto, t2.Cod_Sucursal, 0 AS Stock FROM Producto_Unico AS t1
# MAGIC CROSS JOIN (SELECT * FROM Sucursales WHERE Cod_Sucursal_PK <> 0) AS t2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Se realiza un JOIN entre la view productos_distintos con la tabla intermedia Productos_sucursales para poder poblar el stock de productos sucursales
# MAGIC SELECT t1.Cod_Producto, t2.Stock_Inicial
# MAGIC FROM Producto_Unico AS t1
# MAGIC INNER JOIN productos_distintos AS t2
# MAGIC ON t1.Producto = t2.Producto
# MAGIC ORDER BY t1.Cod_Producto;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Creamos la view anterior, productos_stock_inicial, con la consulta anterior
# MAGIC CREATE VIEW productos_stock_inicial AS
# MAGIC SELECT t1.Cod_Producto, t2.Stock_Inicial
# MAGIC FROM Producto_Unico AS t1
# MAGIC INNER JOIN productos_distintos AS t2
# MAGIC ON t1.Producto = t2.Producto
# MAGIC ORDER BY t1.Cod_Producto;

# COMMAND ----------

# DBTITLE 1,                                                   Creamos el dataframe de PySpark
# Se crean dataframes de PySpark con las Manage Tables de Databricks
productos_stock_inicial = sqlContext.table('productos_stock_inicial')
productos_sucursales = sqlContext.table('Productos_Sucursales')
productos_stock_inicial.show(5)
productos_sucursales.show(5)

# COMMAND ----------

# DBTITLE 1,                                        Convierten los dataframe Pyspark en Pandas
# Se convierten los dataframe Pyspark en Pandas
productos_stock_inicial_pd = productos_stock_inicial.toPandas()
productos_sucursales_pd = productos_sucursales.toPandas()

# COMMAND ----------

# DBTITLE 1,                                                             Itera sobre filas
# Se itera sobre cada una de las filas de productos sucursales
for index, row in productos_sucursales_pd.iterrows():
    # A la columna stock de productos sucursales se le asigna un número random entre 0 y el conteo de items duplicados (conteo de items en la tabla de productos)
    row['Stock'] = random.randint(0,productos_stock_inicial_pd[productos_stock_inicial_pd['Cod_Producto'] == row['Cod_Producto']]['Stock_Inicial'].values[0])

# COMMAND ----------

productos_sucursales_pd['Stock_Inicial'] = productos_sucursales_pd['Stock'] 

# COMMAND ----------

# DBTITLE 1,                                                                    Visualización
display(productos_sucursales_pd)

# COMMAND ----------

# Tranformar dataframe panda a dataframe Spark
productos_sucursales=spark.createDataFrame(productos_sucursales_pd)

# COMMAND ----------

# Se lee la tabla intermedia Producto_Unico, y se guarda el resultado en el dataframe_producto_unico
dataframe_producto_unico = sqlContext.sql('select * from Producto_Unico')

# COMMAND ----------

# Se guarda el dataframe_producto_unico, que contiene la tabla intermedia llena, en la base de datos mediante el conector jdbc
df_produnico_todb = DataFrameWriter(dataframe_producto_unico)
df_produnico_todb.jdbc(url=jdbcUrl, table= "Producto_Unico", mode ="overwrite")

# COMMAND ----------

# Se guarda el productos_sucursales_pd, que contiene la tabla intermedia llena, en la base de datos mediante el conector jdbc

df_prodsucursales_todb = DataFrameWriter(productos_sucursales)
df_prodsucursales_todb.jdbc(url=jdbcUrl, table= "Productos_Sucursales", mode ="overwrite")
