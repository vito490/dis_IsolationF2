name := "esempi"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "junit" % "junit" % "3.8.1",
  "org.apache.spark" %% "spark-core" % "2.2.0",
  "org.apache.spark" %% "spark-sql" % "2.2.0",
  "com.databricks" %% "spark-csv" % "1.5.0"
)
