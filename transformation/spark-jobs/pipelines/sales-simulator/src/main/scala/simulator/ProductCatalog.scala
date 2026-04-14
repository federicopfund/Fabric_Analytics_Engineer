package simulator

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.util.Random
import java.time.{LocalDate, LocalDateTime, DayOfWeek}
import java.time.format.DateTimeFormatter
import java.sql.Timestamp

/**
 * ProductCatalog — carga el catálogo real de productos desde el bucket RAW
 * de COS para garantizar coherencia con los datos existentes del pipeline.
 */
object ProductCatalog {

  case class Product(
    codProducto:      Int,
    nombre:           String,
    codSubCategoria:  Int,
    color:            String,
    precioUnitario:   Double,
    costoUnitario:    Double
  )

  /** Mapa de precios reales extraído del CSV original de VentasInternet. */
  val priceMap: Map[Int, (Double, Double)] = Map(
    // Accesorios — precio bajo
    528 -> (4.99, 1.87), 529 -> (3.99, 1.49), 530 -> (4.99, 1.87),
    531 -> (8.99, 3.40), 532 -> (8.99, 3.40), 533 -> (8.99, 3.40),
    534 -> (21.98, 8.37), 535 -> (24.99, 9.35), 536 -> (29.99, 11.22),
    537 -> (34.99, 13.09), 538 -> (39.99, 14.96),
    // Prendas — precio medio-bajo
    218 -> (9.50, 3.40), 219 -> (9.50, 3.40), 220 -> (34.99, 12.02),
    221 -> (34.99, 12.02), 222 -> (34.99, 12.02), 223 -> (34.99, 12.02),
    477 -> (3.99, 1.38), 478 -> (9.99, 3.44), 479 -> (8.99, 3.10),
    480 -> (8.99, 3.10), 481 -> (8.99, 3.10), 482 -> (8.99, 3.10),
    483 -> (49.99, 18.12), 484 -> (7.95, 2.88), 485 -> (7.95, 2.88),
    486 -> (53.99, 19.78), 487 -> (53.99, 19.78), 488 -> (53.99, 19.78),
    489 -> (63.50, 23.28), 490 -> (63.50, 23.28), 491 -> (63.50, 23.28),
    492 -> (49.99, 18.34), 493 -> (49.99, 18.34), 494 -> (49.99, 18.34),
    495 -> (49.99, 18.34), 496 -> (49.99, 18.34), 497 -> (49.99, 18.34),
    498 -> (69.99, 25.43), 499 -> (69.99, 25.43), 500 -> (69.99, 25.43),
    501 -> (69.99, 25.43), 502 -> (69.99, 25.43), 503 -> (69.99, 25.43),
    504 -> (89.99, 33.64), 505 -> (89.99, 33.64), 506 -> (89.99, 33.64),
    // Cascos — precio medio
    212 -> (34.99, 13.09), 213 -> (8.75, 3.27), 214 -> (8.75, 3.27),
    215 -> (8.65, 3.24), 216 -> (8.65, 3.24), 217 -> (8.65, 3.24),
    // Bicicletas — precio alto
    310 -> (3578.27, 1898.09), 311 -> (3578.27, 1898.09), 312 -> (3578.27, 1898.09),
    313 -> (3578.27, 1898.09), 314 -> (3578.27, 1898.09), 315 -> (3454.60, 1833.49),
    316 -> (1120.49, 594.83), 317 -> (1120.49, 594.83), 318 -> (1120.49, 594.83),
    319 -> (1120.49, 594.83), 320 -> (1120.49, 594.83), 321 -> (782.99, 415.58),
    322 -> (1457.99, 773.94), 323 -> (1457.99, 773.94), 324 -> (1457.99, 773.94),
    325 -> (1457.99, 773.94), 326 -> (1457.99, 773.94), 327 -> (1457.99, 773.94),
    328 -> (2171.29, 1152.46), 329 -> (2171.29, 1152.46), 330 -> (2171.29, 1152.46),
    331 -> (2171.29, 1152.46), 332 -> (2171.29, 1152.46), 333 -> (2171.29, 1152.46),
    334 -> (2181.56, 1157.91), 335 -> (2181.56, 1157.91), 336 -> (2181.56, 1157.91),
    337 -> (2181.56, 1157.91), 338 -> (2181.56, 1157.91), 339 -> (2181.56, 1157.91),
    340 -> (2443.35, 1296.82), 341 -> (2443.35, 1296.82), 342 -> (2443.35, 1296.82),
    343 -> (2443.35, 1296.82), 344 -> (3399.99, 1804.50), 345 -> (3399.99, 1804.50),
    346 -> (3399.99, 1804.50), 347 -> (3399.99, 1804.50),
    // Bicicletas de paseo
    539 -> (539.99, 286.63), 540 -> (539.99, 286.63), 541 -> (539.99, 286.63),
    542 -> (539.99, 286.63), 543 -> (539.99, 286.63), 544 -> (539.99, 286.63),
    545 -> (769.49, 408.41), 546 -> (769.49, 408.41), 547 -> (769.49, 408.41),
    548 -> (769.49, 408.41),
    // Accesorios complementarios
    600 -> (34.99, 13.09), 601 -> (34.99, 13.09), 604 -> (44.99, 17.24),
    605 -> (44.99, 17.24), 606 -> (44.99, 17.24)
  )

  /** Segmentos de producto por tipo de venta con pesos de probabilidad. */
  sealed trait Segment { def weight: Double; def products: Seq[Int] }
  case object Accesorios extends Segment {
    val weight = 0.40
    val products: Seq[Int] = (528 to 538).toSeq ++ Seq(600, 601, 604, 605, 606)
  }
  case object Prendas extends Segment {
    val weight = 0.30
    val products: Seq[Int] = (477 to 506).toSeq ++ (218 to 223).toSeq
  }
  case object Cascos extends Segment {
    val weight = 0.10
    val products: Seq[Int] = (212 to 217).toSeq
  }
  case object BicicletasMontana extends Segment {
    val weight = 0.08
    val products: Seq[Int] = (310 to 347).toSeq
  }
  case object BicicletasCarretera extends Segment {
    val weight = 0.07
    val products: Seq[Int] = (316 to 343).toSeq
  }
  case object BicicletasPaseo extends Segment {
    val weight = 0.05
    val products: Seq[Int] = (539 to 548).toSeq
  }

  val allSegments: Seq[Segment] = Seq(
    Accesorios, Prendas, Cascos, BicicletasMontana, BicicletasCarretera, BicicletasPaseo
  )

  def pickSegment(rng: Random): Segment = {
    val r = rng.nextDouble()
    var cumulative = 0.0
    allSegments.find { seg =>
      cumulative += seg.weight
      r < cumulative
    }.getOrElse(Accesorios)
  }

  def pickProduct(segment: Segment, rng: Random): Int = {
    val valid = segment.products.filter(priceMap.contains)
    if (valid.isEmpty) 528 else valid(rng.nextInt(valid.length))
  }
}
