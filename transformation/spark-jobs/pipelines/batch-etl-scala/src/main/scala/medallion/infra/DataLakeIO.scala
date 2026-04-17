package medallion.infra

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import io.delta.tables.DeltaTable

/**
 * Utilidades de lectura/escritura para el Datalake.
 * Soporta CSV (ingesta), Parquet (Bronze/Silver) y Delta Lake (Gold).
 * Detecta automáticamente rutas locales vs HDFS.
 */
object DataLakeIO {

  private val logger = Logger.getLogger(getClass.getName)

  def readCsv(spark: SparkSession, basePath: String, fileName: String, schema: StructType): DataFrame = {
    val fullPath = s"$basePath/$fileName"
    logger.info(s"Leyendo: $fullPath")
    spark.read
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS")
      .schema(schema)
      .csv(fullPath)
  }

  def writeParquet(df: DataFrame, basePath: String, tableName: String, partitionCol: Option[String] = None): Unit = {
    val outputPath = s"$basePath/$tableName"
    logger.info(s"Escribiendo Parquet: $outputPath")
    val writer = df.write.mode("overwrite")
    partitionCol match {
      case Some(col) => writer.partitionBy(col).parquet(outputPath)
      case None      => writer.parquet(outputPath)
    }
    logger.info(s"✔ $tableName escrito correctamente")
  }

  def writeDelta(df: DataFrame, basePath: String, tableName: String, partitionCol: Option[String] = None): Unit = {
    val outputPath = s"$basePath/$tableName"
    logger.info(s"Escribiendo Delta: $outputPath")
    val writer = df.write.mode("overwrite").format("delta")
    partitionCol match {
      case Some(col) => writer.partitionBy(col).save(outputPath)
      case None      => writer.save(outputPath)
    }
    logger.info(s"✔ $tableName (Delta) escrito correctamente")
  }

  /**
   * Sobrescribe SOLO las filas que cumplen `predicate` (replaceWhere).
   * Útil para agregaciones particionadas por tiempo (ej. KPIs mensuales):
   * solo reescribe los meses tocados, sin reescribir el histórico completo.
   *
   * Si la tabla aún no existe, hace un write completo (bootstrap).
   *
   * @param df         DataFrame con SOLO las filas a reemplazar
   * @param basePath   Ruta base del datalake
   * @param tableName  Nombre de la tabla
   * @param predicate  Expresión SQL que selecciona el rango a reemplazar (ej. "anio >= 2025")
   */
  def writeDeltaReplaceWhere(df: DataFrame, basePath: String, tableName: String, predicate: String): Unit = {
    val outputPath = s"$basePath/$tableName"
    if (!pathExists(outputPath)) {
      logger.info(s"Delta replaceWhere: $tableName no existe, haciendo write completo (bootstrap)")
      writeDelta(df, basePath, tableName)
      return
    }
    logger.info(s"Delta replaceWhere: $tableName — predicate=[$predicate]")
    df.write
      .mode("overwrite")
      .format("delta")
      .option("replaceWhere", predicate)
      .save(outputPath)
    logger.info(s"✔ $tableName (Delta replaceWhere) escrito correctamente")
  }

  def writeDeltaOrParquet(df: DataFrame, basePath: String, tableName: String, partitionCol: Option[String] = None): Unit = {
    try {
      writeDelta(df, basePath, tableName, partitionCol)
    } catch {
      case _: Exception =>
        logger.warn(s"Delta write falló para $tableName, usando Parquet fallback")
        writeParquet(df, basePath, tableName, partitionCol)
    }
  }

  /**
   * Incremental MERGE (upsert) en tabla Delta existente.
   * Si la tabla no existe, hace un write completo.
   *
   * @param df           DataFrame con datos nuevos/actualizados
   * @param basePath     Ruta base del datalake
   * @param tableName    Nombre de la tabla
   * @param mergeKeys    Columnas que forman la clave de merge (AND condition)
   * @param partitionCol Columna opcional de partición
   */
  def mergeDelta(
    df: DataFrame,
    basePath: String,
    tableName: String,
    mergeKeys: Seq[String],
    partitionCol: Option[String] = None
  ): Unit = {
    val outputPath = s"$basePath/$tableName"

    if (!pathExists(outputPath)) {
      logger.info(s"Delta MERGE: $tableName no existe, haciendo write completo")
      writeDelta(df, basePath, tableName, partitionCol)
      return
    }

    logger.info(s"Delta MERGE: $tableName — upsert por [${mergeKeys.mkString(", ")}]")

    val deltaTable = DeltaTable.forPath(outputPath)

    val mergeCondition = mergeKeys.map(k => s"target.$k = source.$k").mkString(" AND ")

    deltaTable.as("target")
      .merge(df.as("source"), mergeCondition)
      .whenMatched.updateAll()
      .whenNotMatched.insertAll()
      .execute()

    logger.info(s"✔ $tableName (Delta MERGE) completado")
  }

  // ---------------------------------------------------------------------------
  // Estrategia adaptativa de escritura: overwrite vs MERGE incremental.
  //
  // Reglas (la primera que se cumpla gana):
  //   1) `PIPELINE_GOLD_FORCE_MERGE=true`           -> MERGE
  //   2) `<TABLE>_MERGE_THRESHOLD` (env, override)  -> compara contra esa cota
  //   3) `PIPELINE_MERGE_THRESHOLD_ROWS` (default)  -> compara contra cota global
  //   4) Cota por defecto del parámetro `thresholdRows`
  //
  // Si la tabla actual tiene >= cota → MERGE (cheap delta).
  // Si la tabla actual tiene  < cota → overwrite full (más simple, idempotente).
  // ---------------------------------------------------------------------------

  sealed trait WriteStrategy
  case object FullOverwrite extends WriteStrategy
  case object IncrementalMerge extends WriteStrategy

  /** Cuenta filas usando metadata de Delta (no escanea archivos). */
  def countDeltaRows(basePath: String, tableName: String): Long = {
    val outputPath = s"$basePath/$tableName"
    if (!pathExists(outputPath)) return 0L
    try SparkSession.active.read.format("delta").load(outputPath).count()
    catch { case e: Exception => logger.warn(s"countDeltaRows($tableName) falló: ${e.getMessage}"); 0L }
  }

  private def envLong(key: String): Option[Long] =
    sys.env.get(key).flatMap(v => scala.util.Try(v.toLong).toOption)

  /** Decide la estrategia de escritura para una tabla Gold. */
  def chooseWriteStrategy(
    basePath: String,
    tableName: String,
    thresholdRows: Long
  ): WriteStrategy = {
    val forceMerge = sys.env.getOrElse("PIPELINE_GOLD_FORCE_MERGE", "false").equalsIgnoreCase("true")
    if (forceMerge) {
      logger.info(s"$tableName: estrategia=MERGE (PIPELINE_GOLD_FORCE_MERGE=true)")
      return IncrementalMerge
    }
    val perTableKey = s"${tableName.toUpperCase}_MERGE_THRESHOLD"
    val effectiveThreshold =
      envLong(perTableKey)
        .orElse(envLong("PIPELINE_MERGE_THRESHOLD_ROWS"))
        .getOrElse(thresholdRows)

    val current = countDeltaRows(basePath, tableName)
    if (current >= effectiveThreshold) {
      logger.info(s"$tableName: estrategia=MERGE (current=$current >= threshold=$effectiveThreshold)")
      IncrementalMerge
    } else {
      logger.info(s"$tableName: estrategia=OVERWRITE (current=$current < threshold=$effectiveThreshold)")
      FullOverwrite
    }
  }

  /**
   * Escribe o mergea según tamaño actual de la tabla. Critérios en `chooseWriteStrategy`.
   *
   * @param df            DataFrame completo (overwrite) o delta (merge)
   * @param basePath      Ruta base Gold
   * @param tableName     Nombre tabla Delta
   * @param mergeKeys     Claves de match para MERGE (ej. Seq("orden_id"))
   * @param thresholdRows Cota por defecto si no hay env var (5M para facts, 1M para dims)
   * @param partitionCol  Columna opcional de partición
   */
  def writeOrMergeDelta(
    df: DataFrame,
    basePath: String,
    tableName: String,
    mergeKeys: Seq[String],
    thresholdRows: Long = 5000000L,
    partitionCol: Option[String] = None
  ): Unit = {
    chooseWriteStrategy(basePath, tableName, thresholdRows) match {
      case FullOverwrite    => writeDelta(df, basePath, tableName, partitionCol)
      case IncrementalMerge => mergeDelta(df, basePath, tableName, mergeKeys, partitionCol)
    }
    logLastOperationMetrics(basePath, tableName)
  }

  /**
   * Audita la última operación de la tabla Delta (qué cambió en el run).
   * Usa el commit log de Delta (operationMetrics) sin costo extra.
   */
  def logLastOperationMetrics(basePath: String, tableName: String): Unit = {
    val outputPath = s"$basePath/$tableName"
    if (!pathExists(outputPath)) return
    try {
      val dt = DeltaTable.forPath(outputPath)
      val rows = dt.history(1).collect()
      rows.headOption.foreach { row =>
        val version = row.getAs[Long]("version")
        val op      = row.getAs[String]("operation")
        val ts      = row.getAs[java.sql.Timestamp]("timestamp")
        val metrics = Option(row.getAs[Map[String, String]]("operationMetrics")).getOrElse(Map.empty)
        val summary = Seq("numOutputRows", "numTargetRowsInserted", "numTargetRowsUpdated",
                          "numTargetRowsDeleted", "numFiles", "numRemovedFiles")
          .flatMap(k => metrics.get(k).map(v => s"$k=$v")).mkString(" ")
        logger.info(s"📊 AUDIT $tableName v$version op=$op ts=$ts $summary")
      }
    } catch { case e: Exception => logger.warn(s"AUDIT $tableName falló: ${e.getMessage}") }
  }

  /**
   * Ejecuta VACUUM en una tabla Delta para limpiar archivos obsoletos.
   * @param basePath      Ruta base del datalake
   * @param tableName     Nombre de la tabla
   * @param retentionHours Horas de retención (default 168 = 7 días)
   */
  def vacuumDelta(basePath: String, tableName: String, retentionHours: Double = 168.0): Unit = {
    val outputPath = s"$basePath/$tableName"
    if (!pathExists(outputPath)) {
      logger.warn(s"VACUUM: $tableName no existe, skip")
      return
    }

    logger.info(f"VACUUM: $tableName — retención ${retentionHours}%.0f horas")
    val deltaTable = DeltaTable.forPath(outputPath)
    deltaTable.vacuum(retentionHours)
    logger.info(s"✔ $tableName (VACUUM) completado")
  }

  /**
   * Ejecuta VACUUM sobre múltiples tablas Delta. Errores individuales se loguean
   * pero no abortan el barrido (best-effort cleanup).
   *
   * Activado por la variable de entorno `PIPELINE_VACUUM=true`. Por defecto desactivado
   * para no encarecer cada run; se recomienda agendarlo semanalmente.
   */
  def vacuumDeltaTables(basePath: String, tableNames: Seq[String], retentionHours: Double = 168.0): Unit = {
    val enabled = sys.env.getOrElse("PIPELINE_VACUUM", "false").equalsIgnoreCase("true")
    if (!enabled) {
      logger.info("VACUUM deshabilitado (setear PIPELINE_VACUUM=true para activar)")
      return
    }
    logger.info(s"═══ VACUUM batch sobre ${tableNames.size} tablas Delta ═══")
    tableNames.foreach { t =>
      try vacuumDelta(basePath, t, retentionHours)
      catch { case e: Exception => logger.warn(s"VACUUM $t falló: ${e.getMessage}") }
    }
  }

  /**
   * true si se debe saltar la reconstrucción de una tabla porque ya existe.
   *
   * Cuando `PIPELINE_FORCE_REFRESH=true`, siempre devuelve false para forzar
   * el reprocesamiento (necesario cuando llegan ventas nuevas a raw y queremos
   * que se propaguen a bronze/silver/gold sin borrar las tablas manualmente).
   *
   * Por defecto activado para que los runs en batch siempre reprocesen.
   * Setear `PIPELINE_FORCE_REFRESH=false` solo si querés comportamiento idempotente.
   */
  def shouldSkip(filePath: String): Boolean = {
    val forceRefresh = sys.env.getOrElse("PIPELINE_FORCE_REFRESH", "true").equalsIgnoreCase("true")
    if (forceRefresh) false else pathExists(filePath)
  }

  def pathExists(filePath: String): Boolean = {
    if (filePath.startsWith("hdfs://") || filePath.startsWith("s3a://") || filePath.startsWith("s3://")) {
      try {
        val uri = new java.net.URI(filePath)
        val conf = SparkSession.active.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(uri, conf)
        val p = new Path(filePath)
        if (!fs.exists(p)) return false
        if (fs.isFile(p)) return true
        fs.exists(new Path(filePath, "_SUCCESS")) ||
          fs.exists(new Path(filePath, "_delta_log"))
      } catch {
        case _: Exception => false
      }
    } else {
      val p = java.nio.file.Paths.get(filePath)
      if (!java.nio.file.Files.exists(p)) return false
      if (java.nio.file.Files.isRegularFile(p)) return true
      java.nio.file.Files.exists(p.resolve("_SUCCESS")) ||
        java.nio.file.Files.exists(p.resolve("_delta_log"))
    }
  }
}
