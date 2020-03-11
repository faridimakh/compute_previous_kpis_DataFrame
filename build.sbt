name := "compute_previous_Kpis"

version := "0.1"
val spark_version = "2.3.1"
scalaVersion := "2.11.8"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % spark_version,
  "com.typesafe" % "config" % "1.3.1",
  "org.scala-lang" % "scala-library" % scalaVersion.value
)
mainClass in assembly := Some("main_class")
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
