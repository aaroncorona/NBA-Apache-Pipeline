import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val sparkSession = SparkSession.builder()
      .appName("CSV Processing Application")
      .master("local[*]") // Use all available cores on the local machine
      .getOrCreate()

    NbaCsvProcessor.loadCSV(sparkSession)

    spark.stop()
  }
}
