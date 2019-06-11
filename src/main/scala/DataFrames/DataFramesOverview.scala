package DataFrames

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

// Optional: Use the following code below to set the Error reporting

object App {
  def main(args : Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("DataFramesOverview")
      .master("local[*]")
      .getOrCreate()

    // Read in file
    val df = spark.read.option("header","true").option("inferSchema","true").csv("src/main/scala/DataFrames/CitiGroup2006_2008.csv")

    // First 5 lines
    df.head(5).foreach(println)

    println("\n")

    // Get column names
    df.columns.foreach(println)

    println("\n")

    // Print Schema
    df.printSchema()

    // Describe DataFrame Numerical Columns
    df.describe().show()

    // Select columns .transform().action()
    df.select("Volume").show()

    // Multiple Columns
    df.select("Date","Close").show(2)

    // Creating New Columns
    val df2 = df.withColumn("HighPlusLow",df("High")-df("Low"))
    // Show result
    df2.columns
    df2.printSchema()

    // Recheck Head
    df2.head(5)

    // Renaming Columns (and selecting some more)
    df2.select(df2("HighPlusLow").as("HPL"),df2("Close")).show()


  }

}