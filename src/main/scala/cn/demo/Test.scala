package cn.demo

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("test").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    import org.apache.spark.sql.functions._

//    val value: RDD[(List[Int], Int)] = sc.parallelize(List((List(1, 2), 32), (List(1, 2), 21), (List(1, 2), 2), (List(3, 2), 5), (List(4, 2), 2), (List(3, 2), 8)))
//    value.reduceByKey(_+_).foreach(println)
    val rdd = sc.parallelize(List("admin,demo ?? ,  cp  v  "))
    rdd.map(x=>x.split(" ")).foreach(x=> println(x.toList))
    rdd.map(x=>x.split(" ",-1)).foreach(x=> println(x.toList))

  }

}
