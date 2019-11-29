package packfar.staicvalues
import packfar.staicvalues.static_val._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode}

object functions {
  def duplicate_rows_specific_previous_month_num(df: DataFrame, pa: Int): DataFrame = {
    //    var dfbis=df
    import org.apache.spark.sql.functions._
    import spark.implicits._
    df.createOrReplaceTempView("vu")
    val maxDATE_ACTION = "'" + spark.sql("select max(DATE_ACTION) from vu ").collect().map(u => u(0)).toList.head + "'"
    var df1 = df.withColumn("DATE_ACTION", add_months(col("DATE_ACTION"), pa)).filter("DATE_ACTION <= " + maxDATE_ACTION)
    Seq("IND_NB_USER_DST", "Low", "High")
      .foreach(x => df1 = df1.withColumn(x, lit(0)))
    val df3 = df.union(df1)
    val df4: Dataset[Row] = df3.orderBy($"IND_NB_USER_DST".desc).dropDuplicates(key_rows)
    df4
  }
  def assemble_dfs_duplicated(df: DataFrame, month_num: List[Int] = basics_kpis_prvious_months): DataFrame = {
    import spark.implicits._
    var BDDF: DataFrame = duplicate_rows_specific_previous_month_num(df, month_num.sortWith(_ > _)(1))
    month_num.sortWith(_ > _).drop(2).foreach(x => BDDF = BDDF.union(duplicate_rows_specific_previous_month_num(df, x)))
    BDDF.orderBy($"IND_NB_USER_DST".desc).dropDuplicates(key_rows)
    BDDF
  }

  def save_df(df: DataFrame, nb_partition: Int = 1, format_saving: String = "com.databricks.spark.csv", path: String, namedf: String): Unit = {
    df.coalesce(nb_partition).write.mode(SaveMode.Overwrite).format(format_saving).option("header", "true")
      .save(path + namedf)
  }

}
