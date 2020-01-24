package packfar.staicvalues

import org.apache.spark.sql.{DataFrame, SparkSession}

object static_val {
  lazy val spark: SparkSession = SparkSession.
    builder.master("local[*]")
    .appName(s"OneVsRestExample")
    .getOrCreate()
  lazy val appleDF: DataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/aaple.csv")

  val key_cols = List("DATE_ACTION", "ID_STRUCTURE", "CD_POSTE_TYPE")
  val kpis_cols = List("IND_NB_USER_DST", "Low", "High")
  val list_months_to_compute=List(1,3,4)
}
