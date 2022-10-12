package cn.demo.exam

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Goods_log {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("goods")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val fileRDD: RDD[String] = sc.textFile("hdfs://single03:9000/app/data/exam/goods_log/returned_goods_log_7-9.csv")
//    fileRDD.foreach(println)

    val logRDD: RDD[Array[String]] = fileRDD.filter(x => x.startsWith("月份") == false).map(x => x.split(",", -1)).filter(x => x.size == 17)

    //产生售后服务的订单号个数
    val orderNum: Long = logRDD.map(x => x(3)).count()
//    println(orderNum)

    //每个月分别产生售后服务的不同订单个数
//    logRDD.map(x => (x(0), x(3))).distinct().map(x=>(x._1,1)).reduceByKey(_+_).foreach(println)

    //多次售后服务的订单及对应的售后服务次数
//    logRDD.map(x=>(x(3),1)).reduceByKey(_+_).filter(_._2>1).foreach(println)

    //每个月每种售后服务问题类型占当月总售后服务量的占比,（月份，问题类型，占比）
    /*val month_count: RDD[(String, Int)] = logRDD.map(x => (x(0), 1)).reduceByKey(_ + _)
    logRDD.map(x=>(x(0),x(5))).join(month_count).map(x=>((x._1,x._2._1,x._2._2),1)).reduceByKey(_+_)
      .map(x=>(x._1._1,x._1._2,(x._2.toDouble/x._1._3.toDouble).formatted("%.3f"))).foreach(println)*/

    //每个月每个员工处理的任务状态为已完成状态的售后服务数量
    logRDD.map(x=>((x(0),x(12)),x(13))).filter(_._2.equals("已完成")).distinct().countByKey().map(x=>(x._1._1,x._1._2,x._2)).foreach(println)
  }

}
