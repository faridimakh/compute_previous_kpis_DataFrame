package packfar.staicvalues

import packfar.staicvalues.functions._
import packfar.staicvalues.static_val.{kpis_cols, spark, _}

object main_class {
  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("WARN")

    //    compute :
    val res = calule_previous_kpis(appleDF, key_cols, kpis_cols, List(1,3,4))

    //    save result:
    save_df(res, 1, "/home/farid/Bureau/JavaSimple", "kpisResultAaple")

    //    test result:
    val check_result_df = res.select(key_cols.head, res.columns.filter(x => x.startsWith("IND_NB_USER_DST")): _*)
      .groupBy(key_cols.head).sum()
    check_result_df.orderBy(key_cols.head).show()
  }

}
