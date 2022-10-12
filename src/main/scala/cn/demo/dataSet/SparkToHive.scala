package cn.demo.dataSet

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkToHive {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkToHive")
    val sparkApp: SparkSession = SparkSession.builder().config(conf)
      .config("hive.metastore.uris","thrift://single03:9083")
      .enableHiveSupport().getOrCreate()

//    val df: DataFrame = sparkApp.sql("select * from toronto")
//    df.show()

    val df: DataFrame = sparkApp.sql("select * from lalian.orders limit 5")
  }

}
