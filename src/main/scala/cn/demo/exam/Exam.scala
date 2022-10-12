package cn.demo.exam

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Exam {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("train")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    import spark.implicits._
    import org.apache.spark.sql.functions._

    val trainRDD: RDD[String] = sc.textFile("hdfs://single03:9000/exam/train2/train2.txt")
    val trainDF: DataFrame = trainRDD.map(x => {
      val fields: Array[String] = x.split("\\|")
      (fields(0), fields(1), fields(2), fields(3), fields(17), fields(26), fields(35), fields(54))
    }).toDF("ATPType", "TrainID", "TrainNum", "AttachRWBureau", "AtpError", "BaliseError", "SignalError", "Humidity")
//    trainDF.show(10,false)

    val timeRDD: RDD[String] = sc.textFile("hdfs://single03:9000/exam/timeData/列车出厂时间数据.txt")
    val timeDF: DataFrame = timeRDD.map(x => {
      val fields = x.split("\\|")
      (fields(0), fields(1))
    }).toDF("trainID", "date")
//    timeDF.show(10,false)











  }

}
