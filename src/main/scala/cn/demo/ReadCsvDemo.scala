package cn.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ReadCsvDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("cacheDemo")
    val sparkApp: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc:SparkContext= SparkContext.getOrCreate(conf)

    /*
     SparkContext 操作读取csv文件
    val line: RDD[String] = sc.textFile("input/users.csv")
    println(line.count())

    //去除首行
    val line2: RDD[String] = line.mapPartitionsWithIndex((n, v) => {
      if (n == 0)
        v.drop(1)
      else {
        v
      }
    })
    println(line2.count())
    val line3: RDD[Array[String]] = line2.map(x => x.split(","))
//    line3.collect().foreach(x=>println(x))
    for(x<-line3)
      println(x.toList)*/


    val df: DataFrame = sparkApp.read.format("csv")
      .option("header", "true") //header 第一行不作为数据内容，作为标题
      .load("input/users.csv") //加载数据
//    df.printSchema()
//    df.show()

    df.select("user_id","birthyear").show(3)

  }
}
