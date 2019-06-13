package Classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes

//  In this project we will be working with a fake advertising data set, indicating whether or not a particular internet user clicked on an Advertisement. We will try to create a model that will predict whether or not they will click on an ad based off the features of that user.
//  This data set contains the following features:
//    'Daily Time Spent on Site': consumer time on site in minutes
//    'Age': cutomer age in years
//    'Area Income': Avg. Income of geographical area of consumer
//    'Daily Internet Usage': Avg. minutes a day consumer is on the internet
//    'Ad Topic Line': Headline of the advertisement
//    'City': City of consumer
//    'Male': Whether or not consumer was male
//    'Country': Country of consumer
//    'Timestamp': Time at which consumer clicked on Ad or closed window
//    'Clicked on Ad': 0 or 1 indicated clicking on Ad


object LogRegExample2 {
  def main(args : Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("LogRegExample2")
      .master("local[*]")
      .getOrCreate()

    // Prepare training and test data.
    val data = spark.read.option("header","true").option("inferSchema","true").format("csv").load("src/main/scala/Classification/advertising.csv")

    // Print the Schema of the DataFrame
    data.printSchema()

    ///////////////////////
    /// Display Data /////
    /////////////////////

    // Print out a sample row of the data (multiple ways to do this)
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

    //   Do the Following:
    //    - Rename the Clicked on Ad column to "label"
    //    - Grab the following columns "Daily Time Spent on Site","Age","Area Income","Daily Internet Usage","Timestamp","Male"
    //    - Create a new column called Hour from the Timestamp containing the Hour of the click

    val timedata = data.withColumn("Hour", data("Timestamp").substr(10,2).cast(DataTypes.DoubleType))

    val logregdata = (timedata.select(data("Clicked on Ad").as("label"),
      data("Daily Time Spent on Site"), data("Age"), data("Area Income"),
      data("Daily Internet Usage"),timedata("Hour"),data("Male")))



    // Create a new VectorAssembler object called assembler for the feature
    // columns as the input Set the output column to be called features
    val assembler = (new VectorAssembler()
      .setInputCols(Array("Daily Time Spent on Site", "Age", "Area Income",
        "Daily Internet Usage","Hour"))
      .setOutputCol("features") )

    // Use randomSplit to create a train test split of 70/30
    val Array(training, test) = logregdata.randomSplit(Array(0.7, 0.3), seed = 12345)

    ///////////////////////////////
    // Set Up the Pipeline ///////
    /////////////////////////////

    // Import Pipeline
    import org.apache.spark.ml.Pipeline

    // Create a new LogisticRegression object called lr
    val lr = new LogisticRegression()

    // Create a new pipeline with the stages: assembler, lr
    val pipeline = new Pipeline().setStages(Array(assembler, lr))

    // Fit the pipeline to training set.
    val model = pipeline.fit(training)

    // Get Results on Test Set with transform
    val results = model.transform(test)

    ////////////////////////////////////
    //// MODEL EVALUATION /////////////
    //////////////////////////////////

    // For Metrics and Evaluation import MulticlassMetrics
    import org.apache.spark.mllib.evaluation.MulticlassMetrics

    // Convert the test results to an RDD using .as and .rdd
    import spark.implicits._
    val predictionAndLabels = results.select("prediction","label").as[(Double, Double)].rdd

    // Instantiate a new MulticlassMetrics object
    val metrics = new MulticlassMetrics(predictionAndLabels)

    // Print out the Confusion matrix
    println("Confusion matrix:")
    println(metrics.confusionMatrix)

  }
}