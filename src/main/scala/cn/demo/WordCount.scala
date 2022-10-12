package cn.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark01").setMaster("local[*]")
    val sc: SparkContext = SparkContext.getOrCreate(conf)
        val rdd1: RDD[String] = sc.textFile("input/hello.txt")
//    val rdd1: RDD[String] = sc.textFile("hdfs://single03:9000/kb/hello.txt")
//    rdd1.collect().foreach(println)
    val rdd2: RDD[(String, Int)] = rdd1.flatMap(x => x.split(" ")).map((_, 1)).reduceByKey(_ + _)
    //血缘关系
    println(rdd2.toDebugString)
    rdd2.collect().foreach(println)
//    rdd2.saveAsTextFile("hdfs://single03:9000/kb/out")
  }

}
