package DataAnalysis

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

/** Count up how many of each word occurs in a book, using regular expressions and sorting the final results */
object WordCount {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using the local machine
    val sc = new SparkContext("local", "WordCount")

    // Load each line of my book into an RDD
    val input = sc.textFile("src/main/scala/DataAnalysis/book.txt")

    // Split using a regular expression that extracts words
    val words = input.flatMap(_.split("\\W+"))

    // Normalize everything to lowercase
    val lowercaseWords = words.map(_.toLowerCase())

    // Count of the occurrences of each word
    val wordCounts = lowercaseWords.map(x => (x, 1)).reduceByKey( (x,y) => x + y )

    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = wordCounts.map( x => (x._2, x._1) ).sortByKey()

    // Print the results, flipping the (count, word) results to word: count as we go.
    for (result <- wordCountsSorted) {
      val count = result._1
      val word = result._2
      println(s"$word: $count")
    }

  }

}