package medallion.infra

import org.apache.log4j.Logger
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}

/**
 * Circuit Breaker — Protección contra fallos sistémicos en HDFS/IO.
 *
 * Estados:
 *   CLOSED  → Operaciones normales, cuenta fallos consecutivos
 *   OPEN    → Rechaza operaciones inmediatamente (fallo sistémico detectado)
 *   HALF_OPEN → Permite una operación de prueba para verificar recuperación
 *
 * Transiciones:
 *   CLOSED → OPEN:     failures >= threshold
 *   OPEN → HALF_OPEN:  resetTimeMs transcurrido desde último fallo
 *   HALF_OPEN → CLOSED: operación de prueba exitosa
 *   HALF_OPEN → OPEN:   operación de prueba falló
 *
 * @param threshold   Número de fallos consecutivos para abrir el circuito
 * @param resetTimeMs Tiempo en ms para intentar half-open después de abrir
 */
class CircuitBreaker(
  val name: String = "default",
  val threshold: Int = 3,
  val resetTimeMs: Long = 60000L
) {

  private val logger = Logger.getLogger(getClass.getName)

  sealed trait State
  case object Closed extends State
  case object Open extends State
  case object HalfOpen extends State

  private val failures = new AtomicInteger(0)
  private val lastFailureMs = new AtomicLong(0L)
  private val stateRef = new AtomicReference[State](Closed)

  def currentState: State = {
    stateRef.get() match {
      case Open if System.currentTimeMillis() - lastFailureMs.get() >= resetTimeMs =>
        if (stateRef.compareAndSet(Open, HalfOpen)) {
          logger.info(s"⚡ CircuitBreaker[$name]: OPEN → HALF_OPEN (reset timeout elapsed)")
        }
        stateRef.get()
      case other => other
    }
  }

  /**
   * Ejecuta una operación protegida por el circuit breaker.
   * @throws CircuitBreakerOpenException si el circuito está abierto
   */
  def execute[T](operation: => T): T = {
    currentState match {
      case Open =>
        val elapsed = System.currentTimeMillis() - lastFailureMs.get()
        throw new CircuitBreakerOpenException(
          s"CircuitBreaker[$name] OPEN — $failures fallos consecutivos, " +
          s"reset en ${(resetTimeMs - elapsed) / 1000}s"
        )

      case HalfOpen =>
        logger.info(s"⚡ CircuitBreaker[$name]: HALF_OPEN — probando operación...")
        try {
          val result = operation
          if (stateRef.compareAndSet(HalfOpen, Closed)) {
            failures.set(0)
            logger.info(s"⚡ CircuitBreaker[$name]: HALF_OPEN → CLOSED (operación exitosa)")
          }
          result
        } catch {
          case e: Exception =>
            tripOpen(e)
            throw e
        }

      case Closed =>
        try {
          val result = operation
          onSuccess()
          result
        } catch {
          case e: Exception =>
            onFailure(e)
            throw e
        }
    }
  }

  private def onSuccess(): Unit = {
    failures.set(0)
  }

  private def onFailure(e: Exception): Unit = {
    val count = failures.incrementAndGet()
    lastFailureMs.set(System.currentTimeMillis())
    if (count >= threshold) {
      tripOpen(e)
    } else {
      logger.warn(s"⚡ CircuitBreaker[$name]: fallo $count/$threshold — ${e.getMessage}")
    }
  }

  private def tripOpen(e: Exception): Unit = {
    val transitioned = stateRef.compareAndSet(Closed, Open) ||
                        stateRef.compareAndSet(HalfOpen, Open)
    if (transitioned) {
      lastFailureMs.set(System.currentTimeMillis())
      logger.error(s"⚡ CircuitBreaker[$name]: → OPEN — ${failures.get()} fallos consecutivos: ${e.getMessage}")
    }
  }

  def reset(): Unit = {
    failures.set(0)
    stateRef.set(Closed)
  }

  def getFailureCount: Int = failures.get()

  override def toString: String =
    s"CircuitBreaker[$name](state=${stateRef.get()}, failures=${failures.get()}, threshold=$threshold)"
}

class CircuitBreakerOpenException(message: String) extends RuntimeException(message)
