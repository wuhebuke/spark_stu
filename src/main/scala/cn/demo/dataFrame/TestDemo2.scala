package cn.demo.dataFrame

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TestDemo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkDemo").setMaster("local[*]")
    val sparkApp: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = sparkApp.sparkContext
    import sparkApp.implicits._
    import org.apache.spark.sql.functions._

    val orders: DataFrame = sparkApp.read.format("csv").csv("input/orders.csv")
       .withColumnRenamed("_c0", "id")
      .withColumnRenamed("_c1", "date")
      .withColumnRenamed("_c2", "customerID")
      .withColumnRenamed("_c3", "status")



  }

}
