package medallion.infra

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import scala.util.Try

/**
 * ViewScope — Loan pattern para gestión automática de vistas temporales Spark.
 *
 * Garantiza que las vistas se registren al inicio y se liberen al final,
 * eliminando la necesidad de llamar System.gc() manualmente.
 *
 * Uso:
 *   ViewScope.withViews(spark, silverPath, Seq("catalogo", "rentabilidad"), "parquet") {
 *     spark.sql("SELECT ... FROM silver_catalogo ...").write ...
 *   }
 *   // Vistas liberadas automáticamente al salir del bloque
 */
object ViewScope {

  private val logger = Logger.getLogger(getClass.getName)

  /**
   * Registra vistas temporales, ejecuta el bloque, y libera las vistas al finalizar.
   *
   * @param spark    SparkSession activa
   * @param basePath Ruta base de la capa (e.g. config.silverPath)
   * @param tables   Nombres de tablas a registrar como vistas
   * @param format   Formato de lectura ("parquet" o "delta")
   * @param prefix   Prefijo para el nombre de la vista (e.g. "silver_")
   * @param body     Bloque a ejecutar con las vistas disponibles
   * @return         Resultado del bloque
   */
  def withViews[T](
    spark: SparkSession,
    basePath: String,
    tables: Seq[String],
    format: String = "parquet",
    prefix: String = ""
  )(body: => T): T = {
    val viewNames = tables.map(t => s"$prefix$t")

    try {
      // Registrar vistas
      tables.zip(viewNames).foreach { case (table, viewName) =>
        val path = s"$basePath/$table"
        if (DataLakeIO.pathExists(path)) {
          spark.read.format(format).load(path).createOrReplaceTempView(viewName)
          logger.info(s"  ✓ Vista registrada: $viewName")
        } else {
          logger.warn(s"  ✗ Tabla no encontrada: $path")
        }
      }

      // Ejecutar bloque
      body

    } finally {
      // Liberar vistas — siempre ejecuta, incluso si body lanza excepción
      viewNames.foreach { v =>
        Try(spark.catalog.dropTempView(v))
      }
      spark.catalog.clearCache()
      logger.info(s"  ✔ Vistas liberadas: [${viewNames.mkString(", ")}]")
    }
  }

  /**
   * Variante para registrar múltiples tablas de Bronze como vistas sin prefijo.
   */
  def withBronzeViews[T](spark: SparkSession, bronzePath: String, tables: Seq[String])(body: => T): T =
    withViews(spark, bronzePath, tables, "parquet", "")(body)

  /**
   * Variante para registrar tablas Silver con prefijo "silver_".
   */
  def withSilverViews[T](spark: SparkSession, silverPath: String, tables: Seq[String])(body: => T): T =
    withViews(spark, silverPath, tables, "parquet", "silver_")(body)

  /**
   * Variante para registrar tablas Gold (Delta) con prefijo "gold_".
   */
  def withGoldViews[T](spark: SparkSession, goldPath: String, tables: Seq[String])(body: => T): T =
    withViews(spark, goldPath, tables, "delta", "gold_")(body)
}
