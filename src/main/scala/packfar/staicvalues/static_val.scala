package packfar.staicvalues

import org.apache.spark.sql.functions.{col, concat, dayofmonth, dayofyear, lit, trunc}
import org.apache.spark.sql.{DataFrame, SparkSession}

object static_val {
  val spark: SparkSession = SparkSession.builder.master("local[*]").appName(s"OneVsRestExample").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  var appleDF: DataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/home/farid/Téléchargements/MesLivre/scala/apache-spark-2-master/beginning-apache-spark-2-master/chapter5/data/stocks/aapl-2017.csv")
  //je céer un clée (just pour implementer l'algorithme)
  appleDF = appleDF.withColumn("DATE_ACTION", trunc(col("Date"), "mm")) //je tranc la date (mois)
    .withColumn("ID_STRUCTURE", concat(lit("User"), dayofmonth(col("Date")).cast("String"))) //j'ajoute la clé(id structure on la deja)
    .withColumn("CD_POSTE_TYPE", concat(lit("Name"), dayofyear(col("Date")).cast("String"))) //j'ajoute la clé(id structure on la deja)
    .withColumnRenamed("Volume", "IND_NB_USER_DST")
    .select("DATE_ACTION", "ID_STRUCTURE", "CD_POSTE_TYPE", "IND_NB_USER_DST", "Low", "High")

  val key_rows = List("DATE_ACTION", "ID_STRUCTURE", "CD_POSTE_TYPE")
  val basics_kpis = List("IND_NB_USER_DST", "Low", "High")
  val basics_kpis_prvious_months = List(0, 1, 3, 4, 6, 12)

  val final_columns_df: Seq[String] = key_rows ::: basics_kpis ::: basics_kpis_prvious_months.drop(1).map(x => "_PM" + x).flatMap(x => basics_kpis.map(y => y + x))
}
