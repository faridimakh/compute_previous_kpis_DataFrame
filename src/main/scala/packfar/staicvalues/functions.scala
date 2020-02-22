package packfar.staicvalues

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{add_months, col, lit}
import static_val.spark

object functions {

  /**
   *
   * @param df_inpute :your data frame
   * @param keyCols   :columns represent the key of each row in the Dataframe (must contain date type column)
   * @param kpisCols  : columns represent indicators numerical in the Dataframe (actual KPis)
   * @param numMonths :list of previous months that you hope calculate indicators (list of integers example List(1,2,3) mean
   *                  compute mes privious kpi,1month ago, 2 month ago and 3 month ego)
   * @return
   */
  def calule_previous_kpis(df_inpute: DataFrame, keyCols: List[String], kpisCols: List[String], numMonths: List[Int]): DataFrame = {
    val columns_df_result: Seq[String] = keyCols ::: kpisCols ::: List(0 :: numMonths).flatten.drop(1)
      .map(x => "_PM" + x).flatMap(x => kpisCols.map(y => y + x))
    //estract max date
    df_inpute.createOrReplaceTempView("vu")
    val maxDATE_ACTION = "'" + spark.sql("select max(" + keyCols.head + ") from vu ").collect().map(u => u(0)).toList.head + "'"
    //selct only key columns
    var df_variable = df_inpute.select(keyCols.map(col): _*)
    val df_fix = df_inpute.select(keyCols.map(col): _*) //
    //duplication rows
    List(0 :: numMonths).flatten.foreach(x => df_variable = df_variable.union(df_fix.withColumn(keyCols.head, add_months(col(keyCols.head), x))
      .filter(keyCols.head + " <= " + maxDATE_ACTION)))
    //add null values kpis for keys duplicated
    kpisCols.foreach(x => df_variable = df_variable.withColumn(x, lit(0.0)))
    var df1 = df_inpute.union(df_variable) //add df0
    //drop duplacated, before must sort by colums keys
    df1 = df1.sort(keyCols.map(x => col(x).asc): _*).dropDuplicates(keyCols)
    //final result
    List(0 :: numMonths).flatten
      .map(x => df1.withColumn(keyCols.head, add_months(col(keyCols.head), x)))
      .reduceLeft(_.join(_, keyCols, "left"))
      .toDF(columns_df_result: _*).na.fill(0)
      .sort(keyCols.map(x => col(x).asc): _*)
  }

  /**
   *
   * @param save_this_df     :Dataframe to save
   * @param nb_partition     : number of partition on saving 'save_this_df'(default tuned to 1)
   * @param path_saving_df   path where save 'save_this_df'
   * @param name_saving_df   saving 'save_this_df' as this name
   * @param format_saving_df format saving (csv, json...)[default tuned to 'csv']
   * @param mode_saving_df   mode saving (default tuned to 'Overwrite')
   */
  def save_df(save_this_df: DataFrame, nb_partition: Int = 1,
              path_saving_df: String,
              name_saving_df: String,
              format_saving_df: String = "com.databricks.spark.csv",
              mode_saving_df: String = "Overwrite"): Unit = {
    save_this_df.coalesce(nb_partition).
      write.mode(mode_saving_df).
      format(format_saving_df).
      option("header", "true")
      .save(path_saving_df + "/" + name_saving_df)
  }


}
