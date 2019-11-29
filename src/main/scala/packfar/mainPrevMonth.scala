package packfar

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import packfar.staicvalues.functions._
import packfar.staicvalues.static_val._

object mainPrevMonth {
  def main(args: Array[String]): Unit = {

    spark.sparkContext.setLogLevel("WARN")

    //run-------------------------------------------------------------------------------------------------------------------------------
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