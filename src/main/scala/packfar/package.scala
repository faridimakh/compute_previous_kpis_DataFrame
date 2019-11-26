import org.apache.spark.sql.{DataFrame, SparkSession}

package object packfar {

  val spark: SparkSession = SparkSession.builder.master("local[*]").appName(s"OneVsRestExample").getOrCreate()
  spark.sparkContext.setLogLevel("WARN")
  var appleDF: DataFrame = spark.read.format("csv").
    option("header", "true").
    option("inferSchema", "true").
    load("/home/farid/Téléchargements/MesLivre/scala/apache-spark-2-master/beginning-apache-spark-2-master/chapter5/data/stocks/aapl-2017.csv")

  def duplic_copute_prev(df: DataFrame, pa: Int): DataFrame = {
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

  def duplic_bddf(df: DataFrame): DataFrame = {
    import spark.implicits._
    var dfbddf: DataFrame = duplic_copute_prev(df, 12)
    Seq(6, 4, 3, 1).foreach(x => dfbddf = dfbddf.union(duplic_copute_prev(df, x)))
    dfbddf.orderBy($"IND_NB_USER_DST".desc).dropDuplicates("DATE_ACTION", "ID_STRUCTURE", "CD_POSTE_TYPE")
    dfbddf
  }


}
