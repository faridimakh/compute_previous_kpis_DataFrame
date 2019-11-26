import org.apache.spark.sql.{DataFrame, SparkSession}

package object packfar {

  val spark: SparkSession = SparkSession.builder.master("local[*]").appName(s"OneVsRestExample").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  var appleDF: DataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/home/farid/Téléchargements/MesLivre/scala/apache-spark-2-master/beginning-apache-spark-2-master/chapter5/data/stocks/aapl-2017.csv")

  val joint_key = Seq("DATE_ACTION", "ID_STRUCTURE", "CD_POSTE_TYPE")
  val basics_kpis = Seq("IND_NB_USER_DST", "Low", "High")
  val prvious_months_num = Seq(0, 1, 3, 4, 6, 12)

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
    val df4 = df3.orderBy($"IND_NB_USER_DST".desc).dropDuplicates("DATE_ACTION", "ID_STRUCTURE", "CD_POSTE_TYPE")
    df4
  }

  def assemble_dfs_duplicated(df: DataFrame, month_num: List[Int] = prvious_months_num.toList): DataFrame = {
    import spark.implicits._
    var BDDF: DataFrame = duplicate_rows_specific_previous_month_num(df, month_num.sortWith(_ > _)(1))
    month_num.sortWith(_ > _).drop(2).foreach(x => BDDF = BDDF.union(duplicate_rows_specific_previous_month_num(df, x)))
    BDDF.orderBy($"IND_NB_USER_DST".desc).dropDuplicates("DATE_ACTION", "ID_STRUCTURE", "CD_POSTE_TYPE")
    BDDF
  }


}
