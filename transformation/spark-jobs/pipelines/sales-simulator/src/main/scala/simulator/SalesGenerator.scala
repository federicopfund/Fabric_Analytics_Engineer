package simulator

import scala.util.Random
import java.time.{LocalDate, LocalDateTime, DayOfWeek, Month}

/**
 * SalesGenerator — motor de simulación de ventas realistas.
 *
 * Modela patrones reales de retail:
 *   - Estacionalidad mensual (picos en dic/nov, bajas en ene/feb)
 *   - Patrones semanales (más ventas vie/sáb)
 *   - Distribución de clientes por territorio (segmentación geográfica)
 *   - Promociones con probabilidad variable
 *   - Correlación precio-cantidad (productos caros → menos unidades)
 *   - Tiempos de envío realistas con varianza
 */
object SalesGenerator {

  /** Territorios (1-10) con pesos de distribución geográfica. */
  val territorioWeights: Map[Int, Double] = Map(
    1 -> 0.08, 2 -> 0.05, 3 -> 0.06, 4 -> 0.12,
    5 -> 0.04, 6 -> 0.25, 7 -> 0.10, 8 -> 0.08,
    9 -> 0.15, 10 -> 0.07
  )

  /** Promociones con probabilidad de aplicación. */
  val promociones: Seq[(Int, Double)] = Seq(
    (1, 0.70),   // Sin promoción — 70%
    (2, 0.08),   // Descuento volumen — 8%
    (3, 0.06),   // Temporada — 6%
    (4, 0.05),   // Cliente fiel — 5%
    (5, 0.04),   // Liquidación — 4%
    (13, 0.04),  // Black Friday — 4%
    (14, 0.03)   // Cyber Monday — 3%
  )

  /** Factor de estacionalidad mensual (1.0 = base, >1 = pico). */
  def seasonalFactor(month: Int): Double = month match {
    case 1  => 0.60  // Enero — post-fiestas
    case 2  => 0.65  // Febrero
    case 3  => 0.80  // Marzo — inicio primavera
    case 4  => 0.90  // Abril
    case 5  => 1.00  // Mayo
    case 6  => 1.15  // Junio — verano, ciclismo
    case 7  => 1.20  // Julio — pico verano
    case 8  => 1.10  // Agosto
    case 9  => 0.95  // Septiembre
    case 10 => 1.05  // Octubre
    case 11 => 1.30  // Noviembre — Black Friday
    case 12 => 1.50  // Diciembre — Navidad
    case _  => 1.00
  }

  /** Factor por día de la semana (1.0 = base). */
  def dayOfWeekFactor(dow: DayOfWeek): Double = dow match {
    case DayOfWeek.MONDAY    => 0.85
    case DayOfWeek.TUESDAY   => 0.80
    case DayOfWeek.WEDNESDAY => 0.90
    case DayOfWeek.THURSDAY  => 0.95
    case DayOfWeek.FRIDAY    => 1.25
    case DayOfWeek.SATURDAY  => 1.35
    case DayOfWeek.SUNDAY    => 0.90
  }

  def pickTerritorio(rng: Random): Int = {
    val r = rng.nextDouble()
    var cum = 0.0
    territorioWeights.toSeq.sortBy(-_._2).find { case (_, w) =>
      cum += w
      r < cum
    }.map(_._1).getOrElse(6)
  }

  def pickPromocion(rng: Random, month: Int): Int = {
    val adjusted = if (month == 11 || month == 12) {
      // Más promociones en temporada alta
      promociones.map {
        case (1, w)  => (1, w * 0.5)
        case (13, w) => (13, w * 3.0)
        case (14, w) => (14, w * 3.0)
        case (id, w) => (id, w * 1.5)
      }
    } else promociones

    val total = adjusted.map(_._2).sum
    val r = rng.nextDouble() * total
    var cum = 0.0
    adjusted.find { case (_, w) =>
      cum += w
      r < cum
    }.map(_._1).getOrElse(1)
  }

  def pickCantidad(precioUnitario: Double, rng: Random): Int = {
    if (precioUnitario > 1000) 1                            // Bicicletas: siempre 1
    else if (precioUnitario > 100) if (rng.nextDouble() < 0.9) 1 else 2
    else if (precioUnitario > 30)  1 + (rng.nextDouble() * 2).toInt    // 1-2
    else 1 + (rng.nextDouble() * 4).toInt                              // 1-4 accesorios
  }

  def calcImpuesto(ingresoBruto: Double): Double =
    BigDecimal(ingresoBruto * 0.08).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble

  def calcFlete(ingresoBruto: Double, rng: Random): Double = {
    val basePct = 0.02 + rng.nextDouble() * 0.03   // 2%-5% del precio
    BigDecimal(ingresoBruto * basePct).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  /** Genera NumeroOrden con formato SOxxxxx (continúa desde SO75560). */
  def genNumeroOrden(baseOffset: Int, idx: Int): String =
    f"SO${75561 + baseOffset + idx}%d"

  /** Calcula FechaEnvio (3-14 días después de orden). */
  def calcFechaEnvio(fechaOrden: LocalDateTime, rng: Random): LocalDateTime = {
    val dias = 3 + (rng.nextDouble() * 11).toInt   // 3-14 días
    fechaOrden.plusDays(dias)
  }

  /** Calcula FechaVencimiento (FechaEnvio + 5 días). */
  def calcFechaVencimiento(fechaEnvio: LocalDateTime): LocalDateTime =
    fechaEnvio.plusDays(5)

  /** Genera un Cod_Cliente en el rango real 11000-34984. */
  def pickCliente(rng: Random): Int =
    11000 + rng.nextInt(23985)

  /**
   * Genera N ventas para un rango de fechas dado.
   * El número real de ventas se ajusta por estacionalidad y día de semana.
   */
  case class VentaRecord(
    codProducto:      Int,
    codCliente:       Int,
    codTerritorio:    Int,
    numeroOrden:      String,
    cantidad:         Int,
    precioUnitario:   Double,
    costoUnitario:    Double,
    impuesto:         Double,
    flete:            Double,
    fechaOrden:       java.sql.Timestamp,
    fechaEnvio:       java.sql.Timestamp,
    fechaVencimiento: java.sql.Timestamp,
    codPromocion:     Int
  )

  def generate(
    numOrders:     Int,
    startDate:     LocalDate,
    endDate:       LocalDate,
    orderOffset:   Int = 0,
    seed:          Long = System.currentTimeMillis()
  ): Seq[VentaRecord] = {

    val rng = new Random(seed)
    val totalDays = java.time.temporal.ChronoUnit.DAYS.between(startDate, endDate).toInt + 1
    if (totalDays <= 0) return Seq.empty

    // Distribuir órdenes proporcionalmente según estacionalidad
    val dayWeights = (0 until totalDays).map { d =>
      val date = startDate.plusDays(d)
      seasonalFactor(date.getMonthValue) * dayOfWeekFactor(date.getDayOfWeek)
    }
    val totalWeight = dayWeights.sum

    val ordersPerDay = dayWeights.map(w => math.max(1, (w / totalWeight * numOrders).toInt))
    val adjustedTotal = ordersPerDay.sum
    val scaledOrders = if (adjustedTotal == 0) ordersPerDay
    else ordersPerDay.map(o => math.max(1, (o.toDouble / adjustedTotal * numOrders).toInt))

    var globalIdx = 0
    val records = scala.collection.mutable.ArrayBuffer[VentaRecord]()

    for (dayIdx <- 0 until totalDays) {
      val date = startDate.plusDays(dayIdx)
      val nOrders = scaledOrders(dayIdx)

      for (_ <- 0 until nOrders) {
        // Hora aleatoria del día (8:00-23:59)
        val hour = 8 + rng.nextInt(16)
        val minute = rng.nextInt(60)
        val orderTime = date.atTime(hour, minute, rng.nextInt(60))

        // Selección de producto ponderada por segmento
        val segment = ProductCatalog.pickSegment(rng)
        val codProd = ProductCatalog.pickProduct(segment, rng)
        val (precio, costo) = ProductCatalog.priceMap.getOrElse(codProd, (29.99, 11.22))

        // Variación de precio ±3% (descuentos/markup)
        val priceVar = 1.0 + (rng.nextGaussian() * 0.015)
        val precioFinal = BigDecimal(precio * priceVar).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
        val costoFinal  = BigDecimal(costo * priceVar).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble

        val cantidad = pickCantidad(precioFinal, rng)
        val ingresoBruto = cantidad * precioFinal
        val impuesto = calcImpuesto(ingresoBruto)
        val flete = calcFlete(ingresoBruto, rng)

        val fechaEnvio = calcFechaEnvio(orderTime, rng)
        val fechaVencimiento = calcFechaVencimiento(fechaEnvio)

        records += VentaRecord(
          codProducto      = codProd,
          codCliente       = pickCliente(rng),
          codTerritorio    = pickTerritorio(rng),
          numeroOrden      = genNumeroOrden(orderOffset, globalIdx),
          cantidad         = cantidad,
          precioUnitario   = precioFinal,
          costoUnitario    = costoFinal,
          impuesto         = impuesto,
          flete            = flete,
          fechaOrden       = java.sql.Timestamp.valueOf(orderTime),
          fechaEnvio       = java.sql.Timestamp.valueOf(fechaEnvio),
          fechaVencimiento = java.sql.Timestamp.valueOf(fechaVencimiento),
          codPromocion     = pickPromocion(rng, date.getMonthValue)
        )
        globalIdx += 1
      }
    }

    records.toSeq
  }
}
