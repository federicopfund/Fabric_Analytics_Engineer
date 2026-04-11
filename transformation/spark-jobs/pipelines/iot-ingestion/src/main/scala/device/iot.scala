import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import scala.util.Random
import scala.concurrent.{Future, ExecutionContext, Await}
import scala.concurrent.duration._

object DeviceDataProducer {

  val DEVICE_PROFILES = Map(
    "SanRafael" -> Map("temp" -> (51.3, 17.7), "humd" -> (77.4, 18.7), "pres" -> (1019.9, 9.5)),
    "25DeMayo"  -> Map("temp" -> (49.5, 19.3), "humd" -> (33.0, 13.9), "pres" -> (1012.0, 41.3)),
    "Neuquen"   -> Map("temp" -> (63.9, 11.7), "humd" -> (62.8, 21.8), "pres" -> (1015.9, 11.3))
  )

  implicit val ec: ExecutionContext = ExecutionContext.global

  def device(profileName: String, profile: Map[String, (Double, Double)], producer: KafkaProducer[String, String]): Future[Unit] = Future {
    var count = 1

    try {
      while (true) {
        // get random values within a normal distribution of the value
        val temp = profile("temp")._1 + Random.nextGaussian() * profile("temp")._2
        val humd = math.max(0, math.min(profile("humd")._1 + Random.nextGaussian() * profile("humd")._2, 100))
        val pres = profile("pres")._1 + Random.nextGaussian() * profile("pres")._2

        // create CSV structure
        val msg = s"$profileName,${temp},${humd},${pres}"

        // send to Kafka
        producer.send(new ProducerRecord("topic1", msg))
        println(s"sending data to kafka, #$count")
        println(s"sending data to kafka, #$msg")
        count += 1
        Thread.sleep(500)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        println("Error in the Kafka producer loop.")
    }
  }

  def main(args: Array[String]): Unit = {
    val devices = DEVICE_PROFILES.keys.toSeq

    val deviceName = if (args.length == 1 && devices.contains(args(0))) {
      args(0)
    } else {
      println("Please provide a valid device name:")
      devices.foreach(key => println(s"  * $key"))
      print("\nEnter the device name: ")
      scala.io.StdIn.readLine().trim
    }

    val deviceProfile = DEVICE_PROFILES.getOrElse(deviceName, Map.empty)

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    try {
      Await.result(device(deviceName, deviceProfile, producer), Duration.Inf)
    } finally {
      producer.close()
    }
  }
}

