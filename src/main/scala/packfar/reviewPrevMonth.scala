package packfar

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


    val joint_key = Seq("DATE_ACTION", "ID_STRUCTURE", "CD_POSTE_TYPE")
    val basics_kpis = Seq("IND_NB_USER_DST", "Low", "High")
    val prvious_months_num = Seq(1, 3, 4, 6, 12)
    val name_all_kpis = Seq(joint_key ++ basics_kpis ++ prvious_months_num.map(x => "_PM" + x).flatMap(x => basics_kpis.map(y => y + x))).flatten
    //
    //List(duplic_bddf(appleDF),duplic_bddf(appleDF))
    //je dupliquqe
    val v0 = duplic_bddf(appleDF)
    val v1 = v0.withColumn("DATE_ACTION", add_months(col("DATE_ACTION"), 1))
    val v3 = v0.withColumn("DATE_ACTION", add_months(col("DATE_ACTION"), 3))
    val v4 = v0.withColumn("DATE_ACTION", add_months(col("DATE_ACTION"), 4))
    val v6 = v0.withColumn("DATE_ACTION", add_months(col("DATE_ACTION"), 6))
    val v12 = v0.withColumn("DATE_ACTION", add_months(col("DATE_ACTION"), 12))
    val vf = List(v0, v1, v3, v4, v6, v12).reduceLeft(_.join(_, joint_key, joinType = "left")).toDF(name_all_kpis: _*).na.fill(0)

    val test2 = vf.select("DATE_ACTION", "IND_NB_USER_DST", "IND_NB_USER_DST_PM1", "IND_NB_USER_DST_PM3",
      "IND_NB_USER_DST_PM4", "IND_NB_USER_DST_PM6", "IND_NB_USER_DST_PM12")
      .groupBy("DATE_ACTION")
      .sum()
    test2.orderBy("DATE_ACTION").show()

  }
}