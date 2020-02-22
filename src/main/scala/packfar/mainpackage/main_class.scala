package packfar.mainpackage

import packfar.staicvalues.functions.{calule_previous_kpis, save_df}
import packfar.staicvalues.static_val._

object main_class {
  def main(args: Array[String]): Unit = {

    spark.sparkContext.setLogLevel("WARN")
    //    compute :
    val res = calule_previous_kpis(appleDF, key_cols, kpis_cols, list_months_to_compute)

    //    save result:
    save_df(res, 4, output_data, "kpisResultAaple")

    //    test result:
    val check_result_df = res.select(key_cols.head, res.columns.filter(x => x.startsWith("IND_NB_USER_DST")): _*)
      .groupBy(key_cols.head).sum()
    check_result_df.orderBy(key_cols.head).show()
  }

}
