# Databricks notebook source
# MAGIC %md # Transforming Complex Data Types in Spark SQL
# MAGIC 
# MAGIC In this notebook we're going to go through some data transformation examples using Spark SQL. Spark SQL supports many
# MAGIC built-in transformation functions in the module `pyspark.sql.functions` therefore we will start off by importing that.

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# Convenience function for turning JSON strings into DataFrames.
def jsonToDataFrame(json, schema=None):
  # SparkSessions are available with Spark 2.0+
  reader = spark.read
  if schema:
    reader.schema(schema)
  return reader.json(sc.parallelize([json]))

# COMMAND ----------

# MAGIC %md <b>Selecting from nested columns</b> - Dots (`"."`) can be used to access nested columns for structs and maps.

# COMMAND ----------

# Using a struct
schema = StructType().add("a", StructType().add("b", IntegerType()))
                          
events = jsonToDataFrame("""
{
  "a": {
     "b": 1
  }
}
""", schema)

display(events.select("a.b"))

# COMMAND ----------

# Using a map
schema = StructType().add("a", MapType(StringType(), IntegerType()))
                          
events = jsonToDataFrame("""
{
  "a": {
     "b": 1
  }
}
""", schema)

display(events.select("a.b"))

# COMMAND ----------

# MAGIC %md <b>Flattening structs</b> - A star (`"*"`) can be used to select all of the subfields in a struct.

# COMMAND ----------

events = jsonToDataFrame("""
{
  "a": {
     "b": 1,
     "c": 2
  }
}
""")

display(events.select("a.*"))

# COMMAND ----------

# MAGIC %md <b>Nesting columns</b> - The `struct()` function or just parentheses in SQL can be used to create a new struct.

# COMMAND ----------

events = jsonToDataFrame("""
{
  "a": 1,
  "b": 2,
  "c": 3
}
""")

display(events.select(struct(col("a").alias("y")).alias("x")))

# COMMAND ----------

# MAGIC %md <b>Nesting all columns</b> - The star (`"*"`) can also be used to include all columns in a nested struct.

# COMMAND ----------

events = jsonToDataFrame("""
{
  "a": 1,
  "b": 2
}
""")

display(events.select(struct("*").alias("x")))

# COMMAND ----------

# MAGIC %md <b>Selecting a single array or map element</b> - `getItem()` or square brackets (i.e. `[ ]`) can be used to select a single element out of an array or a map.

# COMMAND ----------

events = jsonToDataFrame("""
{
  "a": [1, 2]
}
""")

display(events.select(col("a").getItem(0).alias("x")))

# COMMAND ----------

# Using a map
schema = StructType().add("a", MapType(StringType(), IntegerType()))

events = jsonToDataFrame("""
{
  "a": {
    "b": 1
  }
}
""", schema)

display(events.select(col("a").getItem("b").alias("x")))

# COMMAND ----------

# MAGIC %md <b>Creating a row for each array or map element</b> - `explode()` can be used to create a new row for each element in an array or each key-value pair.  This is similar to `LATERAL VIEW EXPLODE` in HiveQL.

# COMMAND ----------

events = jsonToDataFrame("""
{
  "a": [1, 2]
}
""")

display(events.select(explode("a").alias("x")))

# COMMAND ----------

# Using a map
schema = StructType().add("a", MapType(StringType(), IntegerType()))

events = jsonToDataFrame("""
{
  "a": {
    "b": 1,
    "c": 2
  }
}
""", schema)

display(events.select(explode("a").alias("x", "y")))

# COMMAND ----------

# MAGIC %md <b>Collecting multiple rows into an array</b> - `collect_list()` and `collect_set()` can be used to aggregate items into an array.

# COMMAND ----------

events = jsonToDataFrame("""
[{ "x": 1 }, { "x": 2 }]
""")

display(events.select(collect_list("x").alias("x")))

# COMMAND ----------

# using an aggregation
events = jsonToDataFrame("""
[{ "x": 1, "y": "a" }, { "x": 2, "y": "b" }]
""")

display(events.groupBy("y").agg(collect_list("x").alias("x")))

# COMMAND ----------

# MAGIC %md <b>Selecting one field from each item in an array</b> - when you use dot notation on an array we return a new array where that field has been selected from each array element.

# COMMAND ----------

events = jsonToDataFrame("""
{
  "a": [
    {"b": 1},
    {"b": 2}
  ]
}
""")

display(events.select("a.b"))

# COMMAND ----------

# MAGIC %md <b>Convert a group of columns to json</b> - `to_json()` can be used to turn structs into json strings. This method is particularly useful when you would like to re-encode multiple columns into a single one when writing data out to Kafka. This method is not presently available in SQL.

# COMMAND ----------

events = jsonToDataFrame("""
{
  "a": {
    "b": 1
  }
}
""")

display(events.select(to_json("a").alias("c")))

# COMMAND ----------

# MAGIC %md <b>Parse a column containing json</b> - `from_json()` can be used to turn a string column with json data into a struct. Then you may flatten the struct as described above to have individual columns. This method is not presently available in SQL. 
# MAGIC **This method is available since Spark 2.1**

# COMMAND ----------

events = jsonToDataFrame("""
{
  "a": "{\\"b\\":1}"
}
""")

schema = StructType().add("b", IntegerType())
display(events.select(from_json("a", schema).alias("c")))

# COMMAND ----------

# MAGIC %md Sometimes you may want to leave a part of the JSON string still as JSON to avoid too much complexity in your schema.

# COMMAND ----------

events = jsonToDataFrame("""
{
  "a": "{\\"b\\":{\\"x\\":1,\\"y\\":{\\"z\\":2}}}"
}
""")

schema = StructType().add("b", StructType().add("x", IntegerType())
                            .add("y", StringType()))
display(events.select(from_json("a", schema).alias("c")))

# COMMAND ----------

# MAGIC %md <b>Parse a set of fields from a column containing json</b> - `json_tuple()` can be used to extract a fields available in a string column with json data.

# COMMAND ----------

events = jsonToDataFrame("""
{
  "a": "{\\"b\\":1}"
}
""")

display(events.select(json_tuple("a", "b").alias("c")))

# COMMAND ----------

# MAGIC %md <b>Parse a well formed string column</b> - `regexp_extract()` can be used to parse strings using regular expressions.

# COMMAND ----------

events = jsonToDataFrame("""
[{ "a": "x: 1" }, { "a": "y: 2" }]
""")

display(events.select(regexp_extract("a", "([a-z]):", 1).alias("c")))
