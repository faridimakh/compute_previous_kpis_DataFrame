name := "compute_previous_Kpis"

version := "0.1"
val spark_version = "2.4.4"
scalaVersion := "2.12.0"
libraryDependencies += "org.apache.spark" %% "spark-core" % spark_version
libraryDependencies += "org.apache.spark" %% "spark-sql" % spark_version
