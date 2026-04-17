package medallion.workflow

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.Logger
import medallion.config.{DatalakeConfig, Db2Config, TableRegistry}
import medallion.infra.DataLakeIO
import scala.util.control.NonFatal

/**
 * WORKFLOW: Exportación de tablas Gold → Db2 on Cloud via JDBC.
 *
 * Lee cada tabla Gold (Delta/Parquet) del datalake y la escribe
 * en Db2 usando JDBC bulk insert. Los nombres de tabla en Db2
 * corresponden exactamente a los de la capa Gold:
 *
 *   gold/dim_producto          → Db2.dim_producto
 *   gold/dim_cliente           → Db2.dim_cliente
 *   gold/fact_ventas           → Db2.fact_ventas
 *   gold/kpi_ventas_mensuales  → Db2.kpi_ventas_mensuales
 *   gold/dim_operador          → Db2.dim_operador
 *   gold/fact_produccion_minera→ Db2.fact_produccion_minera
 *   gold/kpi_mineria           → Db2.kpi_mineria
 *
 * Prerequisitos:
 *   - Variables de entorno: DB2_HOSTNAME, DB2_PORT, DB2_DATABASE,
 *     DB2_USERNAME, DB2_PASSWORD
 *   - Driver JDBC: com.ibm.db2.jcc (incluido en build.sbt)
 */
object Db2ExportWorkflow {

  private val logger = Logger.getLogger(getClass.getName)

  private val JDBC_DRIVER = "com.ibm.db2.jcc.DB2Driver"
  private val BATCH_SIZE = 1000
  private val FETCH_SIZE = 1000

  /**
   * Exporta todas las tablas Gold al Db2.
   * Usa overwrite (truncate + insert) para garantizar consistencia.
   *
   * @param spark  SparkSession activa
   * @param config Configuración del datalake (incluye goldPath y db2Config)
   * @return Número de tablas exportadas exitosamente
   */
  def run(spark: SparkSession, config: DatalakeConfig): Int = {
    logger.info("╔══════════════════════════════════════════╗")
    logger.info("║  DB2 EXPORT — Gold Layer → Db2 on Cloud  ║")
    logger.info("╚══════════════════════════════════════════╝")

    val db2 = config.db2Config match {
      case Some(c) => c
      case None =>
        logger.error("✗ Db2 no configurado — se requieren DB2_HOSTNAME y DB2_PASSWORD")
        return 0
    }

    println(s"  🗄️  JDBC URL: ${maskJdbcUrl(db2.jdbcUrl)}")
    println(s"  👤 Usuario: ${db2.username}")
    println(s"  📊 Tablas Gold a exportar: ${TableRegistry.goldNames.length}")
    println()

    // Skip testConnection() — ahorra 10-30s. JDBC write fallará rápido si no hay conexión.

    var exported = 0

    // Exportación en paralelo: cada tabla en su propio thread para solapar I/O Delta read + JDBC write
    val exportParallelism = sys.env.getOrElse("DB2_EXPORT_PARALLELISM", "4").toInt
    val pool = java.util.concurrent.Executors.newFixedThreadPool(exportParallelism, new java.util.concurrent.ThreadFactory {
      private val counter = new java.util.concurrent.atomic.AtomicInteger(0)
      override def newThread(r: Runnable): Thread = {
        val t = new Thread(r, s"db2-export-${counter.incrementAndGet()}")
        t.setDaemon(true)
        t
      }
    })
    val exportedCounter = new java.util.concurrent.atomic.AtomicInteger(0)
    val latch = new java.util.concurrent.CountDownLatch(TableRegistry.goldNames.length)

    TableRegistry.goldNames.foreach { tableName =>
      pool.submit(new Runnable {
        override def run(): Unit = try {
          val tablePath = s"${config.goldPath}/$tableName"

          if (!DataLakeIO.pathExists(tablePath)) {
            logger.warn(s"  ⏭ gold/$tableName no existe — skip")
          } else {
            val startTime = System.currentTimeMillis()

            // Leer tabla Gold y exportar a Db2 (sin count() que hace full-scan extra)
            val df = readGoldTable(spark, tablePath, tableName)

          // Escribir a Db2 via JDBC
          writeToDb2(df, db2, tableName)

          val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
          println(f"  ✔ $tableName — exportado en ${elapsed}%.1fs")
          exportedCounter.incrementAndGet()
          }
        } catch {
          case NonFatal(e) =>
            logger.error(s"  ✗ $tableName — ${e.getMessage}", e)
            println(s"  ✗ $tableName — ERROR: ${e.getMessage}")
        } finally {
          latch.countDown()
        }
      })
    }

    // Esperar a que terminen (timeout 8 min total)
    val finished = latch.await(8, java.util.concurrent.TimeUnit.MINUTES)
    pool.shutdownNow()
    if (!finished) {
      logger.warn("  ⚠ Db2 export timeout — algunas tablas no se exportaron")
    }
    exported = exportedCounter.get()

    println()
    println(s"  ═══ RESULTADO: $exported/${TableRegistry.goldNames.length} tablas exportadas a Db2 ═══")
    logger.info(s"✔ Db2 Export completado: $exported/${TableRegistry.goldNames.length}")
    exported
  }

  /**
   * Exporta una única tabla Gold al Db2.
   */
  def exportTable(spark: SparkSession, config: DatalakeConfig, tableName: String): Boolean = {
    val db2 = config.db2Config.getOrElse {
      logger.error("Db2 no configurado"); return false
    }

    val tablePath = s"${config.goldPath}/$tableName"
    if (!DataLakeIO.pathExists(tablePath)) {
      logger.warn(s"gold/$tableName no existe"); return false
    }

    try {
      val df = readGoldTable(spark, tablePath, tableName)
      writeToDb2(df, db2, tableName)
      true
    } catch {
      case NonFatal(e) =>
        logger.error(s"Error exportando $tableName: ${e.getMessage}", e)
        false
    }
  }

  /**
   * Lee una tabla Gold (intenta Delta primero, fallback a Parquet).
   */
  private def readGoldTable(spark: SparkSession, path: String, name: String): DataFrame = {
    try {
      spark.read.format("delta").load(path)
    } catch {
      case _: Exception =>
        logger.info(s"  $name: Delta no disponible, leyendo como Parquet")
        spark.read.parquet(path)
    }
  }

  /**
   * Escribe un DataFrame a Db2 via JDBC.
   * Usa modo overwrite (truncate + insert) para carga completa.
   * Elimina columnas de auditoría internas (_gold_updated_at) antes de escribir.
   */
  private def writeToDb2(df: DataFrame, db2: Db2Config, tableName: String): Unit = {
    // Eliminar columnas de auditoría internas del datalake
    val auditCols = df.columns.filter(_.startsWith("_"))
    val cleanDf = if (auditCols.nonEmpty) {
      df.drop(auditCols: _*)
    } else df

    val connProperties = new java.util.Properties()
    connProperties.put("user", db2.username)
    connProperties.put("password", db2.password)
    connProperties.put("driver", JDBC_DRIVER)
    connProperties.put("batchsize", BATCH_SIZE.toString)
    connProperties.put("fetchsize", FETCH_SIZE.toString)
    connProperties.put("sslConnection", db2.sslConnection.toString)
    // Timeouts para evitar cuelgues
    connProperties.put("loginTimeout", "30")         // segundos
    connProperties.put("queryTimeout", "300")        // segundos (5 min por query)
    connProperties.put("socketTimeout", "300000")    // ms (5 min)
    connProperties.put("connectTimeout", "30000")    // ms

    cleanDf.write
      .mode("overwrite")
      .option("truncate", "true")           // TRUNCATE en vez de DROP+CREATE para preservar permisos
      .option("batchsize", BATCH_SIZE)
      .option("isolationLevel", "NONE")     // Sin bloqueo de transacción — mucho más rápido
      .option("rewriteBatchedStatements", "true")
      .jdbc(db2.jdbcUrl, tableName.toUpperCase, connProperties)
  }

  /**
   * Verifica conectividad JDBC con Db2.
   */
  private def testConnection(db2: Db2Config): Boolean = {
    try {
      Class.forName(JDBC_DRIVER)
      // Timeout global para DriverManager (evita cuelgues indefinidos)
      java.sql.DriverManager.setLoginTimeout(30)
      val props = new java.util.Properties()
      props.put("user", db2.username)
      props.put("password", db2.password)
      props.put("sslConnection", db2.sslConnection.toString)
      props.put("loginTimeout", "30")
      props.put("connectTimeout", "30000")
      props.put("socketTimeout", "60000")

      val conn = java.sql.DriverManager.getConnection(db2.jdbcUrl, props)
      val valid = conn.isValid(10)
      conn.close()

      if (valid) {
        println(s"  ✔ Conexión Db2 verificada")
        logger.info("Db2 connection test: OK")
      }
      valid
    } catch {
      case NonFatal(e) =>
        logger.error(s"Db2 connection test falló: ${e.getMessage}")
        println(s"  ✗ Conexión Db2 falló: ${e.getMessage}")
        false
    }
  }

  /**
   * Enmascara la URL JDBC para log seguro (oculta hostname parcialmente).
   */
  private def maskJdbcUrl(url: String): String = {
    url.replaceAll("""(jdbc:db2://)[^:]+""", "$1****")
  }
}
