package packfar.staicvalues

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.sys.process._

object static_val {

  //-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  final val my_conf: Config = ConfigFactory.load("application.conf")
  final lazy val input_data: String = my_conf.getString("data.input")
  final lazy val output_save_path: String = my_conf.getString("data.output")
  final lazy val name_file_output_result: String = my_conf.getString("data.name_file_output_result")
  final lazy val spark: SparkSession = new SparkSession.Builder().appName(my_conf.getString("spark.name")).master(my_conf.getString("spark.master")).getOrCreate()

  //-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  //dataFrame info
  final lazy val appleDF: DataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(input_data)
  lazy val key_cols = List("DATE_ACTION", "ID_STRUCTURE", "CD_POSTE_TYPE")
  lazy val kpis_cols = List("IND_NB_USER_DST", "Low", "High")

  //-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  //the previous months that you want compute compute
  lazy val list_months_to_compute: List[Int] = Set(1,1,9,5,5,1,2,2,2,7).toList.sorted //we use Set for bock duplicate months

  //-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  //shell commands for cleanning path storage:

  lazy val clean_storage_before_next_process: Int = Seq("rm", "-r", output_save_path + "/").!
  lazy val clean_unnecessary_files_after_storage_process_result: Int = Seq("find", output_save_path, "-not", "-name", "*.csv", "-type", "f", "-delete").!


}
