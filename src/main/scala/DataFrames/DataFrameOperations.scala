package DataFrames

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

// Optional: Use the following code below to set the Error reporting

object DataFrameOperations {
  def main(args : Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("DataFrameOperations")
      .master("local[*]")
      .getOrCreate()

    // Read in file
    val df = spark.read.option("header","true").option("inferSchema","true").csv("src/main/scala/DataFrames/CitiGroup2006_2008.csv")

    // Show Schema
    df.printSchema()

    // Show head
    df.head(5).foreach(println)

    //////////////////////////
    //// FILTERING DATA //////
    //////////////////////////

    // This import is needed to use the $-notation
    import spark.implicits._

    // Grabbing all rows where a column meets a condition
    df.filter($"Close" > 480).show()

    // Can also use SQL notation
    df.filter("Close > 480").show()

    // Count how many results
    println(df.filter($"Close" > 480).count())

    // Multiple Filters
    // Note the use of triple === , this may change in the future!
    df.filter($"High"===484.40).show()

    df.filter($"Close"<480 && $"High"<480).show()

    // Collect results into a scala object (Array)
    val High484 = df.filter($"High"===484.40).collect()


  }

}