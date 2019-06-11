name := "scala_ML_implementations"

version := "0.1"

scalaVersion := "2.11.8"
resolvers  += "MavenRepository" at "http://central.maven.org/maven2"

libraryDependencies ++= {
  val sparkVersion = "2.2.1"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion,
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-mllib" % sparkVersion,
    "org.apache.spark" %% "spark-streaming" % sparkVersion,
    "org.apache.spark" %% "spark-hive" % sparkVersion
  )
}