package packfar

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object reviewPrevMonth {
  def main(args: Array[String]): Unit = {

    spark.sparkContext.setLogLevel("WARN")
    //je céer un clée (just pour implementer l'algorithme)
    appleDF = appleDF.withColumn("DATE_ACTION", trunc(col("Date"), "mm")) //je tranc la date (mois)
      .withColumn("ID_STRUCTURE", concat(lit("User"), dayofmonth(col("Date")).cast("String"))) //j'ajoute la clé(id structure on la deja)
      .withColumn("CD_POSTE_TYPE", concat(lit("Name"), dayofyear(col("Date")).cast("String"))) //j'ajoute la clé(id structure on la deja)
      .withColumnRenamed("Volume", "IND_NB_USER_DST")
      .select("DATE_ACTION", "ID_STRUCTURE", "CD_POSTE_TYPE", "IND_NB_USER_DST", "Low", "High")
    //
    val final_columns_df = key_rows ::: basics_kpis ::: basics_kpis_prvious_months.drop(1).map(x => "_PM" + x).flatMap(x => basics_kpis.map(y => y + x))
    //test-------------------------------------------------------------------------------------------------------------------------------
    val kl = assemble_dfs_duplicated(appleDF)
    var op: List[DataFrame] = List[DataFrame]()
    basics_kpis_prvious_months.sortWith(_ > _).foreach(x => op = op.+:(kl.withColumn("DATE_ACTION", add_months(col("DATE_ACTION"), x))))
    val vf = op.reduceLeft(_.join(_, key_rows, joinType = "left")).toDF(final_columns_df: _*).na.fill(0)
    //test-------------------------------------------------------------------------------------------------------------------------------

    val test2 = vf.select("DATE_ACTION", "IND_NB_USER_DST", "IND_NB_USER_DST_PM1", "IND_NB_USER_DST_PM3", "IND_NB_USER_DST_PM4", "IND_NB_USER_DST_PM6", "IND_NB_USER_DST_PM12")
      .groupBy("DATE_ACTION")
      .sum()
    test2.orderBy("DATE_ACTION").show()

  }
}