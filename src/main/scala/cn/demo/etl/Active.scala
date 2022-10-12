package cn.demo.etl

import cn.demo.JdbcUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Active {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("etlDemo").setMaster("local[*]")
    val sparkApp = SparkSession.builder().config(conf).getOrCreate()

    import sparkApp.implicits._
    import org.apache.spark.sql.functions._

    val logs: DataFrame = JdbcUtils.getDataFrameByTableName(sparkApp, "full_log")
    val ds: Dataset[Row] = logs.filter($"actionName" === "BuyCourse" || $"actionName" === "StartLearn")
//    ds.show(false)
//    ds.printSchema()

    val ds2: Dataset[(String, String)] = ds.map(x =>
      (x.getAs[String]("userUID"), x.getAs[String]("events_time").substring(0, 10)
      )
    )
//    ds2.printSchema()
//    ds2.show(false)

    val rstDF: DataFrame = ds2.withColumnRenamed("_1", "userUID")
      .withColumnRenamed("_2", "date")
      .groupBy($"date")
      .agg(countDistinct("userUID").as("activeNum"))
//    rstDF.show(false)

    JdbcUtils.saveDataToMysql(rstDF,"activeNum")

  }

}
