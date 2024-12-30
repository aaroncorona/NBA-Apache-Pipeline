import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD

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
    val dfRaw = sparkSession.read
      .format(fileType)
      .option("inferSchema", inferSchema)
      .option("header", firstRowIsHeader)
      .option("sep", delimiter)
      .load(fileLocation)
    val df = dfRaw.withColumn("Salary", col("Salary").cast("double"))
    println("Raw data:")
    df.show(5)

    // Get metrics on player ages by team
    val playerAges = df.select("Team", "Age")
      .filter(col("Age").isNotNull)
      .groupBy("Team")
      .agg(round(avg("Age"), 2).alias("Average Age"), max("Age").alias("Max Age"))
      println("Ages detail by Team:")
      playerAges.show(5);
    
    // Transform to an RDD for more low level control for processing salaries
    val rdd: RDD[(String, Double)] = df.rdd.map(row => (row.getAs[String]("Team"), row.getAs[Double]("Salary")))
    val aggregatedRdd: RDD[(String, (Double, Int))] = rdd.mapValues(salary => (salary, 1))
      .reduceByKey((acc, value) => (acc._1 + value._1, acc._2 + value._2)) // Sum up salaries and count players

    // Show the salary details per team
    val avgSalaryRdd: RDD[(String, Double, Double)] = aggregatedRdd.map {
      case (team, (totalSalary, playerCount)) => (team, totalSalary, totalSalary / playerCount)
    }
    println("Salary detail by Team:")
    avgSalaryRdd.collect().foreach {
      case (team, totalSalary, avgSalary) => println(s"Team: $team, Total Salary: $$${"%.2f".format(totalSalary)}, Average Salary: $$${"%.2f".format(avgSalary)}")
    }
  }
}
