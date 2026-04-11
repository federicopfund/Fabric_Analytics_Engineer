import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StringType, StructField, StructType, DoubleType, LongType}
import org.apache.log4j.{Level, Logger}
import scala.collection.mutable.ListBuffer


object SparkKafkaStreamingApp {
   // Configuración del logger
  val log: Logger = Logger.getLogger(getClass.getName)
  // Define el esquema para el DataFrame
  val schema = StructType(
    Seq(
      StructField("timestamp", LongType, true),
      StructField("profileName", StringType, true),
      StructField("temp", DoubleType, true),
      StructField("humd", DoubleType, true),
      StructField("pres", DoubleType, true)
    )
  )

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    
    // Configuración de Spark
    val conf = new SparkConf()
                      .setAppName("SparkKafkaStreamingApp")
                      .setMaster("local[*]")
                      .set("spark.executor.cores", "4")  
                      .set("spark.executor.instances", "3")  
                      .set("spark.executor.memory", "2g")
                      .set("spark.driver.extraJavaOptions", "-Djava.version=11")
                      .set("spark.executor.extraJavaOptions", "-Djava.version=11")

    val ssc = new StreamingContext(conf, Seconds(5))  // Intervalo de 5 segundos
   
    
    // Configuración de Kafka
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "spark-consumer-group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean),  "max.poll.records" -> "500",          // Ajusta según tus necesidades
      "session.timeout.ms" -> "30000",      
      "heartbeat.interval.ms" -> "5000",    
      "max.poll.interval.ms" -> "600000"
      )

    // Tópicos de Kafka a los que queremos suscribirnos
    val topics = Array("topic1")

    // Crear el DStream que recibe los datos de Kafka
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
    )

    // Acumulador de fil
    // Procesar los datos del dispositivo
    kafkaStream.foreachRDD { rdd =>
      rdd.foreach { record =>
        val message = record.value()
        val messageData = message.split(",").map(_.trim)

        // Asumimos que los datos siempre tienen cinco partes: timestamp, profileName, temp, humd, pres
        if (messageData.length == 4) {
          val Array(profileName, temp, humd, pres) = messageData
          val row = Row(profileName, temp.toDouble, humd.toDouble, pres.toDouble)
          // Registrar información en el log
          log.info(s"Processed message:  $profileName, $temp, $humd, $pres")
         
          // Puedes realizar otras transformaciones o acciones en el DataFrame aquí según tus necesidades
        }
  
      }

    }

    // Iniciar el streaming
    ssc.start()
    ssc.awaitTermination()
  }
}

