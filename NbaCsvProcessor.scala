import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object NbaCsvProcessor {
  def loadCSV(sparkSession: SparkSession): Unit = {

    // File location and type
    val fileLocation = "/FileStore/tables/nba.csv"
    val fileType = "csv"
    
    // CSV options
    val inferSchema = "false"
    val firstRowIsHeader = "false"
    val delimiter = ","
    
    // Load CSV file into a DataFrame
    val df = sparkSession.read
      .format(fileType)
      .option("inferSchema", inferSchema)
      .option("header", firstRowIsHeader)
      .option("sep", delimiter)
      .load(fileLocation)
    
    df.show()
  }
}
