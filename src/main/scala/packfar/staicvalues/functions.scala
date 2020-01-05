package packfar.staicvalues
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import packfar.staicvalues.static_val._

object functions {
  def duplicate_rows_specific_previous_month_num(df: DataFrame, pa: Int): DataFrame = {
    //    var dfbis=dfz
    import org.apache.spark.sql.functions._
    import spark.implicits._
    df.createOrReplaceTempView("vu")
    val maxDATE_ACTION = "'" + spark.sql("select max(DATE_ACTION) from vu ").collect().map(u => u(0)).toList.head + "'"
    var df1 = df.withColumn("DATE_ACTION", add_months(col("DATE_ACTION"), pa)).filter("DATE_ACTION <= " + maxDATE_ACTION)
    basics_kpis.foreach(x => df1 = df1.withColumn(x, lit(0)))
    val df3 = df.union(df1)
    val df4: Dataset[Row] = df3.orderBy($"IND_NB_USER_DST".desc).dropDuplicates(key_rows)
    df4
  }
  def assemble_dfs_duplicated(df: DataFrame, month_num: List[Int] = actuels_and_prvious_kpis_months_indices): DataFrame = {
    import spark.implicits._
    var BDDF: DataFrame = duplicate_rows_specific_previous_month_num(df, month_num.sortWith(_ > _)(1))
    month_num.sortWith(_ > _).drop(2).foreach(x => BDDF = BDDF.union(duplicate_rows_specific_previous_month_num(df, x)))
    BDDF.orderBy($"IND_NB_USER_DST".desc).dropDuplicates(key_rows)
    BDDF
  }

}
