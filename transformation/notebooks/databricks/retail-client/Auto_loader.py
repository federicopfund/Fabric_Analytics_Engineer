




@dlt.table
def sales_orders_raw():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .load("/databricks-datasets/retail-org/sales_orders/")
pyspark --master local[4]