package packfar.staicvalues

import packfar.staicvalues.functions._
import packfar.staicvalues.static_val.{kpis_cols, spark, _}

object mainclass {
  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("WARN")

    //    compute:
    val ggg = calulprev(appleDF, key_cols, kpis_cols, List(1, 3, 4, 6, 12))

    //    save result:
    save_df(ggg, 1, "/root/Desktop/all/saveResult", "kpisResultAaple")

    //    test result:
    val check_result_df = ggg.select(key_cols.head, ggg.columns.filter(x => x.startsWith("IND_NB_USER_DST")): _*)
      .groupBy(key_cols.head).sum()
    check_result_df.orderBy(key_cols.head).show()
  }

}
