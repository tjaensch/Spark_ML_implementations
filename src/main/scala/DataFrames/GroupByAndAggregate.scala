package DataFrames

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


// Optional: Use the following code below to set the Error reporting

object GroupByAndAggregate {
  def main(args : Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("DataFrameOperations")
      .master("local[*]")
      .getOrCreate()

    // Read in file
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("src/main/scala/DataFrames/Sales.csv")

    // Show Schema
    df.printSchema()

    // Show
    df.show()

    // Groupby Categorical Columns
    // Optional, usually won't save to another object
    df.groupBy("Company")

    // Mean
    df.groupBy("Company").mean().show()
    // Count
    df.groupBy("Company").count().show()
    // Max
    df.groupBy("Company").max().show()
    // Min
    df.groupBy("Company").min().show()
    // Sum
    df.groupBy("Company").sum().show()

    // Other Aggregate Functions
    // http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.functions$
    df.select(countDistinct("Sales")).show() //approxCountDistinct
    df.select(sumDistinct("Sales")).show()
    df.select(variance("Sales")).show()
    df.select(stddev("Sales")).show() //avg,max,min,sum,stddev
    df.select(collect_set("Sales")).show()

    // OrderBy
    // Ascending
    df.orderBy("Sales").show()

    // Descending
    df.orderBy(desc("Sales")).show()

  }
}