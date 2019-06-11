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

    val df = spark.read.option("header","true").option("inferSchema","true").csv("src/main/scala/DataFrames/CitiGroup2006_2008.csv")

    for(line <- df.head(5)){
      println(line)
    }


  }

}