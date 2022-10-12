package cn.demo.exam

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object ExamCovid19 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("exam")
    val sparkApp: SparkSession = SparkSession.builder().config(conf)
      .config("hive.metastore.uris","thrift://single03:9083")
      .enableHiveSupport().getOrCreate()
    val sc: SparkContext = sparkApp.sparkContext

    val rdd: RDD[String] = sc.textFile("hdfs://single03/app/data/exam/covid19/countrydata.csv")

    //统计每个国家在数据截止统计时的累计确诊人数
//   rdd.map(x=>x.split(",")).map(x=>(x(1),x(3))).filter(_._1=="20200702").foreach(x=>println(x._1,x._2))

//        rdd.map(x=>{
//        var item = x.split(",")
//        (item(4),item(1).toInt)
//        }).reduceByKey((a,b)=>math.max(a,b)).foreach(println)

    //统计全世界在数据截止统计时的总感染人数
    /*rdd.map(x=>{
      val item = x.split(",")
      (item(4),item(1).toInt)
    }).reduceByKey(_+_).foreach(println)*/

       /*rdd.map(x => {
      var item = x.split(",");
      (item(4), item(1).toInt)
    }).reduceByKey((a, b) => math.max(a, b)).foreach(println)*/

//      .reduce((x, y) => ("allCount", x._2 + y._2))

    /*val allCount2: (String, Int) = rdd.map(x => {
      var item = x.split(",");
      (item(4), item(1).toInt)
    }).reduceByKey((a, b) => math.max(a, b)).reduce((x, y) => ("allCount", x._2 + y._2))*/

    /*rdd.map(x => {
      var item = x.split(",")
      (item(4), item(1).toInt)
    }).reduceByKey((a, b) => math.max(a, b)).map(x=>("allCount",x._2)).reduceByKey(_+_).foreach(println)*/

    //统计每个大洲中每日新增确诊人数最多的国家及确诊人数，并输出 20200408 这一天各大洲当日新增确诊人数最多的国家及确诊人数
    /*rdd.map(x=>{
      var item=x.split(",")
      ((item(6),item(3)),(item(4),item(2).toInt))
    }).reduceByKey((x,y)=>{if (x._2>y._2) x else y}).filter(x=>x._1._2=="20200408").foreach(println)*/


    //统计每个大洲中每日累计确诊人数最多的国家及确诊人数，并输出 20200607 这一天各大洲当日累计确诊人数最多的国家及确诊人数
    /*rdd.map(x=>{
      var item=x.split(",")
      ((item(6),item(3)),(item(4),item(1).toInt))
    }).reduceByKey((x,y)=>{
      if (x._2>y._2) x else y
    }).filter(x=>x._1._2=="20200607")
      .foreach(println)*/

    //统计每个大洲每月累计确诊人数，显示 202006 这个月每个大洲的累计确诊人数
    rdd.map(x=> {
      val item = x.split(",")
      ((item(6),item(3).substring(0,6)),item(1).toInt)
    }).reduceByKey(_+_).filter(x=>x._1._2=="202006")
      .map(x=>(x._1._1,x._1._2,x._2))
      .foreach(println)

  }
}
