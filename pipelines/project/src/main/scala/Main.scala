import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val sparkSession = SparkSession.builder()
      .appName("CSV Processing Application")
      .master("local[*]") // Use all available cores on the local machine
      .getOrCreate()

    // Start the Spark batch job to process the CSV
    NbaCsvProcessor.loadCSV(sparkSession)

    // Send some of the data over a Kafka stream
    val kafkaTopic = "nba-salary-topic"
    KafkaProducerService.produceData("OKC", "Russell Westbrook, 25, $10,000,000", kafkaTopic)
    KafkaProducerService.produceData("LAL", "Kobe Bryant, 35, $25,000,000", kafkaTopic)
    KafkaProducerService.closeProducer()
  }
}
