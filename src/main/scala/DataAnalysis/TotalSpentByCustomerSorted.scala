package DataAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object TotalSpentByCustomerSorted {

  /** Convert input data to (customerID, amountSpent) tuples */
  def extractCustomerPricePairs(line: String) = {
    val fields = line.split(",")
    (fields(0).toInt, fields(2).toFloat)
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "TotalSpentByCustomerSorted")

    // Load each line of my book into an RDD
    val input = sc.textFile("src/main/scala/DataAnalysis/customer-orders.csv")

    val mappedInput = input.map(extractCustomerPricePairs)

    val totalByCustomer = mappedInput.reduceByKey( (x,y) => x + y )

    val flipped = totalByCustomer.map( x => (x._2, x._1) )

    val totalByCustomerSorted = flipped.sortByKey()

    val results = totalByCustomerSorted.collect()

    // Print the results.
    results.foreach(println)

  }

}
