import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NbaCsvProcessor {
  def loadCSV(sparkSession: SparkSession): Unit = {

    // File location and type
    val fileLocation = "/FileStore/tables/nba.csv"
    val fileType = "csv"
    
    // CSV options
    val inferSchema = "false"
    val firstRowIsHeader = "true"
    val delimiter = ","
    
    // Load CSV file into a DataFrame
    val df = sparkSession.read
      .format(fileType)
      .option("inferSchema", inferSchema)
      .option("header", firstRowIsHeader)
      .option("sep", delimiter)
      .load(fileLocation)
    println("Raw data:")
    df.show(5)

    // Get metrics on player ages by team
    val playerAges = df.select("Team", "Age")
      .filter(col("Age").isNotNull)
      .groupBy("Team")
      .agg(round(avg("Age"), 2).alias("Average Age"), max("Age").alias("Max Age"))
      println("Ages detail by Team:")
      playerAges.show(5);
  }
}
