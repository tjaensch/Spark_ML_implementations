package DataAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object FlightDelays {
  def main(args : Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("FlightDelays")
      .master("local[*]")
      .getOrCreate()

    // Prepare training and test data.
    val airlines = spark.read.option("header","true").option("inferSchema","true").format("csv").load("src/main/scala/DataAnalysis/airlines.csv")

    // Print the Schema of the DataFrame
    airlines.printSchema()

    // View the entire dataset
    airlines.show()

    // Get the first line
    println(airlines.first())

    // View a few lines
    airlines.take(10).foreach(println)



  }
}