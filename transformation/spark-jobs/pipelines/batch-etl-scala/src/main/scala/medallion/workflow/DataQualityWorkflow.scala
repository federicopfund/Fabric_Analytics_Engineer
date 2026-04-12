package medallion.workflow

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import medallion.infra.DataLakeIO

/**
 * WORKFLOW 4: Data Quality — Validación de calidad por capa.
 *
 * Ejecuta checks de calidad ligeros (sin df.count()) en cada layer:
 *   - Existencia de tablas
 *   - Conformidad de esquema (columnas esperadas vs reales)
 *   - Tasa de nulos en columnas clave (muestreo)
 *   - Detección de duplicados (muestreo)
 *   - Score de calidad compuesto por tabla y por capa
 *
 * Diseñado para ejecutarse después de cada stage sin impacto en memoria.
 */
object DataQualityWorkflow {

  private val logger = Logger.getLogger(getClass.getName)

  case class TableQuality(
    layer: String,
    table: String,
    exists: Boolean,
    columnCount: Int,
    expectedColumns: Int,
    schemaMatch: Boolean,
    nullRatePct: Double,
    duplicateRatePct: Double,
    qualityScore: Double
  )

  private val SAMPLE_SIZE = 100

  /** Ejecuta validación de calidad en una capa completa */
  def validateLayer(
    spark: SparkSession,
    layerName: String,
    layerPath: String,
    tables: Seq[String],
    format: String,
    expectedColumnsMap: Map[String, Int] = Map.empty
  ): Seq[TableQuality] = {
    println()
    println(s"  ╔═══ DATA QUALITY: $layerName ═══╗")

    val results = tables.map { table =>
      val tablePath = s"$layerPath/$table"
      try {
        if (!DataLakeIO.pathExists(tablePath)) {
          println(s"  ✗ $table — NO ENCONTRADA")
          TableQuality(layerName, table, exists = false, 0, 0, schemaMatch = false, 100.0, 0.0, 0.0)
        } else {
          val df = spark.read.format(format).load(tablePath)
          val cols = df.columns
          val colCount = cols.length
          val expectedCols = expectedColumnsMap.getOrElse(table, colCount)
          val schemaMatch = colCount >= expectedCols

          // Muestreo ligero para checks de calidad
          val sample = df.limit(SAMPLE_SIZE)
          val sampleRows = sample.collect()
          val sampleSize = sampleRows.length

          // Tasa de nulos: porcentaje de celdas null en el sample
          val nullRate = if (sampleSize > 0 && colCount > 0) {
            val totalCells = sampleSize.toLong * colCount.toLong
            val nullCells = sampleRows.map(row =>
              (0 until colCount).count(i => row.isNullAt(i))
            ).sum.toLong
            (nullCells.toDouble / totalCells.toDouble) * 100.0
          } else 0.0

          // Duplicados: basado en hash del sample completo
          val duplicateRate = if (sampleSize > 1) {
            val distinct = sampleRows.map(_.toString).distinct.length
            ((sampleSize - distinct).toDouble / sampleSize.toDouble) * 100.0
          } else 0.0

          // Score compuesto: 40% existencia+schema, 30% null rate, 30% duplicate rate
          val existScore = if (schemaMatch) 100.0 else 50.0
          val nullScore = Math.max(0.0, 100.0 - nullRate * 2)
          val dupScore = Math.max(0.0, 100.0 - duplicateRate * 2)
          val qualityScore = Math.round((existScore * 0.4 + nullScore * 0.3 + dupScore * 0.3) * 100) / 100.0

          val status = if (qualityScore >= 90) "✔" else if (qualityScore >= 70) "⚠" else "✗"
          println(f"  $status $table — cols=$colCount schema=${if (schemaMatch) "OK" else "DRIFT"} nulls=$nullRate%.1f%% dups=$duplicateRate%.1f%% score=$qualityScore%.1f")

          TableQuality(layerName, table, exists = true, colCount, expectedCols, schemaMatch,
            Math.round(nullRate * 100) / 100.0, Math.round(duplicateRate * 100) / 100.0, qualityScore)
        }
      } catch {
        case e: Exception =>
          println(s"  ✗ $table — ERROR: ${e.getMessage}")
          TableQuality(layerName, table, exists = false, 0, 0, schemaMatch = false, 100.0, 0.0, 0.0)
      }
    }

    // Liberar memoria del muestreo
    spark.catalog.clearCache()
    System.gc()

    // Resumen de la capa
    val avgScore = if (results.nonEmpty) results.map(_.qualityScore).sum / results.length else 0.0
    val okCount = results.count(_.qualityScore >= 90)
    val warnCount = results.count(r => r.qualityScore >= 70 && r.qualityScore < 90)
    val failCount = results.count(_.qualityScore < 70)
    println(f"  ─── $layerName Quality Score: $avgScore%.1f / 100  |  ✔$okCount ⚠$warnCount ✗$failCount ───")
    println()

    results
  }

  /** Genera un reporte consolidado de calidad en consola */
  def printConsolidatedReport(allResults: Seq[TableQuality]): Unit = {
    println()
    println("╔══════════════════════════════════════════════════════════════╗")
    println("║              DATA QUALITY REPORT — CONSOLIDADO              ║")
    println("╠══════════════════════════════════════════════════════════════╣")

    val byLayer = allResults.groupBy(_.layer)
    byLayer.toSeq.sortBy(_._1).foreach { case (layer, results) =>
      val avgScore = results.map(_.qualityScore).sum / results.length
      val grade = if (avgScore >= 95) "A+" else if (avgScore >= 90) "A" else if (avgScore >= 80) "B" else if (avgScore >= 70) "C" else "D"
      println(f"║  $layer%-8s — Score: $avgScore%5.1f / 100  Grade: $grade%-2s  (${results.length} tablas)")
    }

    val globalScore = if (allResults.nonEmpty) allResults.map(_.qualityScore).sum / allResults.length else 0.0
    val globalGrade = if (globalScore >= 95) "A+" else if (globalScore >= 90) "A" else if (globalScore >= 80) "B" else if (globalScore >= 70) "C" else "D"
    println("╠══════════════════════════════════════════════════════════════╣")
    println(f"║  GLOBAL      — Score: $globalScore%5.1f / 100  Grade: $globalGrade%-2s  (${allResults.length} tablas)")
    println("╚══════════════════════════════════════════════════════════════╝")
    println()
  }
}
