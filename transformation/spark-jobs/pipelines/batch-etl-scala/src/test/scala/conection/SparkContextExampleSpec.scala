package vortex.exelstream

/**
 * A simple test for everyone's favourite wordcount example.
 */
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.SparkSession

class SparkContextExampleSpec extends AnyFlatSpec with Matchers {

  "SparkContextExample" should "load and show DataFrame" in {
    val spark = SparkSession.builder
      .appName("SparkSessionExampleTest")
      .master("local[2]")
      .getOrCreate()

    try {
      // Load data for testing
      val dataFrame = spark.read.csv("./src/main/resources/csv/Categoria.csv")

      // Perform some transformations with Spark
      val result = dataFrame.select("_c0").collect()

      // Assert the result based on your expectations
      result should not be empty
      result.length shouldBe 5 // Adjust based on your data

    } finally {
      // Stop the Spark session
      spark.stop()
    }
  }
}
