package Regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression


// Optional: Use the following code below to set the Error reporting

object LinRegWalkthrough {
  def main(args : Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("LinRegWalkthrough")
      .master("local[*]")
      .getOrCreate()

    // Prepare training and test data.
    val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("src/main/scala/Regression/USA-Housing.csv")

    // Check out the Data
    data.printSchema()

    // See an example of what the data looks like
    // by printing out a Row
    val colnames = data.columns
    val firstrow = data.head(1)(0)
    println("\n")
    println("Example Data Row")
    for(ind <- Range(1,colnames.length)){
      println(colnames(ind))
      println(firstrow(ind))
      println("\n")
    }

    ////////////////////////////////////////////////////
    //// Setting Up DataFrame for Machine Learning ////
    //////////////////////////////////////////////////

    // A few things we need to do before Spark can accept the data!
    // It needs to be in the form of two columns
    // ("label","features")

    // Rename Price to label column for naming convention.
    // Grab only numerical columns from the data
    val df = data.select(data("Price").as("label"),data("Avg Area Income"), data("Avg Area House Age"),data("Avg Area Number of Rooms"),data("Area Population"))

    // An assembler converts the input values to a vector
    // A vector is what the ML algorithm reads to train a model

    // Set the input columns from which we are supposed to read the values
    // Set the name of the column where the vector will be stored
    val assembler = new VectorAssembler().setInputCols(Array("Avg Area Income","Avg Area House Age","Avg Area Number of Rooms","Area Population")).setOutputCol("features")

    // Use the assembler to transform our DataFrame to the two columns
    val output = assembler.transform(df).select("label","features")

    // Create a Linear Regression Model object
    val lr = new LinearRegression()

    // Fit the model to the data

    // Note: Later we will see why we should split
    // the data first, but for now we will fit to all the data.
    val lrModel = lr.fit(output)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics!
    // Explore this in the spark-shell for more methods to call
    val trainingSummary = lrModel.summary

    println(s"numIterations: ${trainingSummary.totalIterations}")
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")

    trainingSummary.residuals.show()

    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"MSE: ${trainingSummary.meanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")


  }
}