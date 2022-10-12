package cn.demo.exam

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ExamAnswerLog {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("exam")
    val sparkApp: SparkSession = SparkSession.builder().config(conf)
      .config("hive.metastore.uris","thrift://single03:9083")
      .enableHiveSupport().getOrCreate()
    val sc: SparkContext = sparkApp.sparkContext
    import sparkApp.implicits._
    import org.apache.spark.sql.functions._

    val rdd: RDD[String] = sc.textFile("hdfs://single03:9000/app/data/exam/answer_question/answer_question.log")
//    rdd.foreach(println)

    val rst: RDD[String] = rdd.map(x => {
      val item = x.split(" ")
      val opt = item(9).replace("r", "").split("_")
      val str = item(10).split(",")
      (opt(1), opt(2), opt(3), str(0)).productIterator.mkString("\t")
    })
    rst.foreach(println)
//    rst.saveAsTextFile("hdfs://single03:9000/app/data/result")

  }
}
