package medallion.config

import medallion.infra.HdfsManager

/**
 * Modelo de configuración inmutable del Datalake.
 * Define las rutas de cada capa del medallion architecture
 * y la configuración de Hadoop/Hive cuando aplica.
 */
case class DatalakeConfig(
  rawPath: String,
  bronzePath: String,
  silverPath: String,
  goldPath: String,
  chartsPath: String = "",
  lineagePath: String = "",
  metricsPath: String = "",
  checkpointPath: String = "",
  hadoopConfig: Option[HdfsManager.HadoopConfig] = None,
  hiveEnabled: Boolean = false
)
