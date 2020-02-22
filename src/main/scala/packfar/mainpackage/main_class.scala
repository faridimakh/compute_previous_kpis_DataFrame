package packfar.mainpackage

import packfar.staicvalues.functions.{calule_previous_kpis, save_df}
import packfar.staicvalues.static_val._

object main_class {

  def main(args: Array[String]): Unit = {

    //-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    // stop logs :
    spark.sparkContext.setLogLevel("WARN")

    //-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    // process :
    val res = calule_previous_kpis(appleDF, key_cols, kpis_cols, list_months_to_compute)
    println("compute previous Kpis ongoing process ...")

    //-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    //cleaning the storage directory before saving
    clean_storage_before_next_process

    //-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    //save result:
    save_df(res, 1, output_save_path, name_file_output_result)

    //-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    //delete unnecessary files from the storage directory
    clean_unnecessary_files_after_storage_process_result
    println("process finish successfully your result file is stored in :" + output_save_path)

    //-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    //uncomment the following lines to check the result :
//    println("verification result ongoing process ...")
//    val check_result_df = res.select(key_cols.head, res.columns.filter(x => x.startsWith("IND_NB_USER_DST")): _*)
//      .groupBy(key_cols.head).sum()
//    check_result_df.orderBy(key_cols.head).show()

  }
}
