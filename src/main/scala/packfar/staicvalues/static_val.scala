package packfar.staicvalues

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

object static_val {
  final val my_conf: Config = ConfigFactory.load("application.conf")
  final lazy val spark: SparkSession = new SparkSession.Builder().appName(my_conf.getString("spark.name"))
    .master(my_conf.getString("spark.master")).getOrCreate()
  final lazy val input_data: String = my_conf.getString("data.input")
  final lazy val output_data: String = my_conf.getString("data.output")
  final lazy val appleDF: DataFrame = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(input_data)
  val key_cols = List("DATE_ACTION", "ID_STRUCTURE", "CD_POSTE_TYPE")
  val kpis_cols = List("IND_NB_USER_DST", "Low", "High")
  val list_months_to_compute=List(1,2,6,9)
}
