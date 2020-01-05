package packfar.staicvalues

import org.apache.spark.sql.{DataFrame, SparkSession}

object static_val {
  val spark: SparkSession = SparkSession.builder.master("local[*]").appName(s"OneVsRestExample").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  var appleDF: DataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("data/aaple.csv")

  val key_rows = List("DATE_ACTION", "ID_STRUCTURE", "CD_POSTE_TYPE")
  val basics_kpis = List("IND_NB_USER_DST", "Low", "High")
  val actuels_and_prvious_kpis_months_indices = List(0, 1, 3, 4, 6, 12)
  val final_columns_df: Seq[String] = key_rows ::: basics_kpis ::: actuels_and_prvious_kpis_months_indices.drop(1).map(x => "_PM" + x).flatMap(x => basics_kpis.map(y => y + x))


}
