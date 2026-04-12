package medallion.schema

import org.apache.spark.sql.types._

/**
 * Esquemas explícitos para los archivos CSV de ingesta.
 * Cubren dos dominios: Retail (bicicletas) y Mining (extracción mineral).
 */
object CsvSchemas {

  // ── RETAIL DOMAIN ──

  val categoriaSchema: StructType = StructType(Seq(
    StructField("Cod_Categoria", IntegerType, nullable = false),
    StructField("Categoria", StringType, nullable = false)
  ))

  val subcategoriaSchema: StructType = StructType(Seq(
    StructField("Cod_SubCategoria", IntegerType, nullable = false),
    StructField("SubCategoria", StringType, nullable = false),
    StructField("Cod_Categoria", IntegerType, nullable = false)
  ))

  val productoSchema: StructType = StructType(Seq(
    StructField("Cod_Producto", IntegerType, nullable = false),
    StructField("Producto", StringType, nullable = true),
    StructField("Cod_SubCategoria", IntegerType, nullable = true),
    StructField("Color", StringType, nullable = true)
  ))

  val ventasInternetSchema: StructType = StructType(Seq(
    StructField("Cod_Producto", IntegerType, nullable = false),
    StructField("Cod_Cliente", IntegerType, nullable = false),
    StructField("Cod_Territorio", IntegerType, nullable = false),
    StructField("NumeroOrden", StringType, nullable = false),
    StructField("Cantidad", IntegerType, nullable = false),
    StructField("PrecioUnitario", DoubleType, nullable = false),
    StructField("CostoUnitario", DoubleType, nullable = false),
    StructField("Impuesto", DoubleType, nullable = true),
    StructField("Flete", DoubleType, nullable = true),
    StructField("FechaOrden", TimestampType, nullable = true),
    StructField("FechaEnvio", TimestampType, nullable = true),
    StructField("FechaVencimiento", TimestampType, nullable = true),
    StructField("Cod_Promocion", IntegerType, nullable = true)
  ))

  val sucursalesSchema: StructType = StructType(Seq(
    StructField("Cod_Sucursal", IntegerType, nullable = false),
    StructField("Cod_Sucursal_PK", IntegerType, nullable = false),
    StructField("Sucursal", StringType, nullable = false),
    StructField("Latitud", DoubleType, nullable = true),
    StructField("Longitud", DoubleType, nullable = true)
  ))

  // ── MINING DOMAIN ──

  val factMineSchema: StructType = StructType(Seq(
    StructField("TruckID", IntegerType, nullable = false),
    StructField("ProjectID", IntegerType, nullable = false),
    StructField("OperatorID", IntegerType, nullable = false),
    StructField("TotalOreMined", DoubleType, nullable = false),
    StructField("TotalWasted", DoubleType, nullable = true),
    StructField("Date", StringType, nullable = true)
  ))

  val mineSchema: StructType = StructType(Seq(
    StructField("TruckID", IntegerType, nullable = false),
    StructField("Truck", StringType, nullable = true),
    StructField("ProjectID", IntegerType, nullable = false),
    StructField("Country", StringType, nullable = true),
    StructField("OperatorID", IntegerType, nullable = false),
    StructField("FirstName", StringType, nullable = true),
    StructField("LastName", StringType, nullable = true),
    StructField("Age", IntegerType, nullable = true),
    StructField("TotalOreMined", DoubleType, nullable = false),
    StructField("TotalWasted", DoubleType, nullable = true),
    StructField("Date", StringType, nullable = true)
  ))
}
