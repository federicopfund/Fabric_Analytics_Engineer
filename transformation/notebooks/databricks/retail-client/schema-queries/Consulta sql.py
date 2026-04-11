
# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM categoria;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM subcategoria;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sucursales;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM producto ORDER BY Cod_Producto;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM (SELECT Producto FROM producto);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM (SELECT DISTINCT Producto FROM producto);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Producto_Unico (
# MAGIC   Cod_Producto bigint GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   Producto VARCHAR(50),
# MAGIC   Cod_Subcategoria INT,
# MAGIC   Cod_Categoria INT
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MIN(Cod_Producto) AS Cod_Producto, Producto, MIN(Cod_SubCategoria) AS Cod_SubCategoria, COUNT(Producto) AS Stock_Inicial FROM producto
# MAGIC GROUP BY Producto
# MAGIC ORDER BY Cod_Producto;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW productos_distintos AS
# MAGIC SELECT MIN(Cod_Producto) AS Cod_Producto, Producto, MIN(Cod_SubCategoria) AS Cod_SubCategoria, COUNT(Producto) AS Stock_Inicial FROM producto
# MAGIC GROUP BY Producto
# MAGIC ORDER BY Cod_Producto;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t1.Cod_Producto, t1.Producto, t1.Cod_SubCategoria, t2.Cod_Categoria
# MAGIC FROM productos_distintos AS t1
# MAGIC INNER JOIN subcategoria AS t2
# MAGIC ON t1.Cod_SubCategoria = t2.Cod_SubCategoria
# MAGIC ORDER BY t1.Cod_Producto;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW productos_unicos_joined AS
# MAGIC SELECT t1.Cod_Producto, t1.Producto, t1.Cod_SubCategoria, t2.Cod_Categoria
# MAGIC FROM productos_distintos AS t1
# MAGIC INNER JOIN subcategoria AS t2
# MAGIC ON t1.Cod_SubCategoria = t2.Cod_SubCategoria
# MAGIC ORDER BY t1.Cod_Producto;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Producto_Unico (Producto, Cod_SubCategoria, Cod_Categoria)
# MAGIC SELECT Producto, Cod_SubCategoria, Cod_Categoria FROM productos_unicos_joined;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Producto_Unico;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t1.Cod_Producto, t2.Cod_Sucursal, 0 AS Stock FROM Producto_Unico AS t1
# MAGIC CROSS JOIN (SELECT * FROM sucursales WHERE Cod_Sucursal_PK <> 0) AS t2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE Productos_Sucursales (
# MAGIC   Cod_Producto INT,
# MAGIC   Cod_Sucursal INT,
# MAGIC   Stock INT);

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Productos_Sucursales
# MAGIC SELECT t1.Cod_Producto, t2.Cod_Sucursal, 0 AS Stock FROM Producto_Unico AS t1
# MAGIC CROSS JOIN (SELECT * FROM sucursales WHERE Cod_Sucursal_PK <> 0) AS t2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t1.Cod_Producto, t2.Stock_Inicial
# MAGIC FROM Producto_Unico AS t1
# MAGIC INNER JOIN productos_distintos AS t2
# MAGIC ON t1.Producto = t2.Producto
# MAGIC ORDER BY t1.Cod_Producto;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VIEW productos_stock_inicial AS
# MAGIC SELECT t1.Cod_Producto, t2.Stock_Inicial
# MAGIC FROM Producto_Unico AS t1
# MAGIC INNER JOIN productos_distintos AS t2
# MAGIC ON t1.Producto = t2.Producto
# MAGIC ORDER BY t1.Cod_Producto;

# COMMAND ----------

productos_stock_inicial = sqlContext.table('productos_stock_inicial')
productos_sucursales = sqlContext.table('Productos_Sucursales')

# COMMAND ----------

productos_stock_inicial.show(5)
productos_sucursales.show(5)

# COMMAND ----------

import random
import pandas as pd

# COMMAND ----------

productos_stock_inicial_pd = productos_stock_inicial.toPandas()
productos_sucursales_pd = productos_sucursales.toPandas()

# COMMAND ----------

for index, row in productos_sucursales_pd.iterrows():
    row['Stock'] = random.randint(0,productos_stock_inicial_pd[productos_stock_inicial_pd['Cod_Producto'] == row['Cod_Producto']]['Stock_Inicial'].values[0])

# COMMAND ----------

display(productos_sucursales_pd)
