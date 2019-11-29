package packfar

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import packfar.staicvalues.functions._
import packfar.staicvalues.static_val._

object mainPrevMonth {
  def main(args: Array[String]): Unit = {

    spark.sparkContext.setLogLevel("WARN")

    //run-------------------------------------------------------------------------------------------------------------------------------
    val df_rows_deflated = assemble_dfs_duplicated(appleDF)

    var list_Dataframs_Indicators_separated: List[DataFrame] = List[DataFrame]()
    actuels_and_prvious_kpis_months_indices.sortWith(_ > _).foreach(x => list_Dataframs_Indicators_separated = list_Dataframs_Indicators_separated.+:(df_rows_deflated.withColumn("DATE_ACTION", add_months(col("DATE_ACTION"), x))))
    val assemble_All_Indicators = list_Dataframs_Indicators_separated.reduceLeft(_.join(_, key_rows, joinType = "left")).toDF(final_columns_df: _*).na.fill(0)

    //test-------------------------------------------------------------------------------------------------------------------------------

    val check_result_df = assemble_All_Indicators.select("DATE_ACTION", "IND_NB_USER_DST", "IND_NB_USER_DST_PM1", "IND_NB_USER_DST_PM3", "IND_NB_USER_DST_PM4", "IND_NB_USER_DST_PM6")
      .groupBy("DATE_ACTION")
      .sum()
    check_result_df.orderBy("DATE_ACTION").show()
  }
}