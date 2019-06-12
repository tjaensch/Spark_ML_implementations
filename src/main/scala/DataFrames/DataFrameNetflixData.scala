package DataFrames

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


// Optional: Use the following code below to set the Error reporting

object DataFrameNetflixData {
  def main(args : Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("DataFrameOperations")
      .master("local[*]")
      .getOrCreate()

    // Read in file
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv("src/main/scala/DataFrames/Netflix_2011_2016.csv")

    // What are the column names?
    df.columns.foreach(println)

    // What does the Schema look like?
    df.printSchema()

    // Print out the first 5 columns.
    df.head(5).foreach(println)

    // Use describe() to learn about the DataFrame.
    df.describe().show()

    // Create a new dataframe with a column called HV Ratio that
    // is the ratio of the High Price versus volume of stock traded
    // for a day.
    val df2 = df.withColumn("HV Ratio",df("High")/df("Volume"))

    // What day had the Peak High in Price?
    df.orderBy(desc("High")).show(1)

    // What is the mean of the Close column?
    df.select(mean("Close")).show()

    // What is the max and min of the Volume column?
    df.select(max("Volume")).show()
    df.select(min("Volume")).show()

    // For Scala/Spark $ Syntax
    import spark.implicits._

    // How many days was the Close lower than $ 600?
    println(df.filter($"Close" < 600).count())

    // What percentage of the time was the High greater than $500 ?
    println((df.filter($"High" > 500).count() * 1.0 / df.count()) * 100)

    // What is the Pearson correlation between High and Volume?
    df.select(corr("High","Volume")).show()

    // What is the max High per year?
    val yeardf = df.withColumn("Year",year(df("Date")))
    val yearmaxs = yeardf.select($"Year",$"High").groupBy("Year").max()
    yearmaxs.select($"Year",$"max(High)").show()

    // What is the average Close for each Calender Month?
    val monthdf = df.withColumn("Month",month(df("Date")))
    val monthavgs = monthdf.select($"Month",$"Close").groupBy("Month").mean()
    monthavgs.select($"Month",$"avg(Close)").show()


  }
}