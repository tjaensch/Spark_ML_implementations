package DataFrames

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


// Optional: Use the following code below to set the Error reporting

object MissingData {
  def main(args : Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("DataFrameOperations")
      .master("local[*]")
      .getOrCreate()

    // Read in file
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("src/main/scala/DataFrames/ContainsNull.csv")

    // Show schema
    df.printSchema()

    // Notice the missing values!
    df.show()

    // Drop any rows with any amount of na values
    df.na.drop().show()

    // Drop any rows that have less than a minimum Number
    // of NON-null values ( < Int)
    df.na.drop(2).show()

    // Fill in the Na values with Int
    df.na.fill(100).show()

    // Fill in String will only go to all string columns
    df.na.fill("Emp Name Missing").show()

    // Be more specific, pass an array of string column names
    df.na.fill("Specific",Array("Name")).show()

    df.describe().show()

    // Now fill in with the values
    df.na.fill(400.5).show()


  }
}