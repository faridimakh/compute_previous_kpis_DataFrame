name := "compute_previous_Kpis"

version := "0.1"
val spark_version = "2.4.4"
scalaVersion := "2.12.0"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % spark_version,
  "com.typesafe" % "config" % "1.3.1",
  "org.scala-lang" % "scala-library" % scalaVersion.value
)