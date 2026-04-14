package medallion.config

import medallion.infra.HdfsManager

/**
 * Modelo de configuración inmutable del Datalake.
 * Define las rutas de cada capa del medallion architecture
 * y la configuración de Hadoop/Hive o IBM COS cuando aplica.
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
  hiveEnabled: Boolean = false,
  cosEnabled: Boolean = false,
  db2Enabled: Boolean = false,
  db2Config: Option[Db2Config] = None
)

/**
 * Configuración de conexión JDBC a Db2 on Cloud.
 * Leída desde variables de entorno.
 */
case class Db2Config(
  hostname: String,
  port: Int,
  database: String,
  username: String,
  password: String,
  sslConnection: Boolean = true
) {
  def jdbcUrl: String = {
    val ssl = if (sslConnection) ":sslConnection=true;" else ";"
    s"jdbc:db2://$hostname:$port/$database$ssl"
  }
}

object Db2Config {
  def fromEnv(): Option[Db2Config] = {
    val hostname = sys.env.getOrElse("DB2_HOSTNAME", "")
    val password = sys.env.getOrElse("DB2_PASSWORD", "")
    if (hostname.nonEmpty && password.nonEmpty) {
      Some(Db2Config(
        hostname = hostname,
        port     = sys.env.getOrElse("DB2_PORT", "30376").toInt,
        database = sys.env.getOrElse("DB2_DATABASE", "bludb"),
        username = sys.env.getOrElse("DB2_USERNAME", ""),
        password = password
      ))
    } else None
  }
}
