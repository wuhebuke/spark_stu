package cn.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CheckPointDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("cacheDemo")
    val sc:SparkContext= SparkContext.getOrCreate(conf)

    sc.setCheckpointDir("./checkPointDemo")
    val rdd1: RDD[(String, Int)] = sc.parallelize(Array(("a", 1), ("b", 2), ("c", 3)))
    rdd1.checkpoint()
    rdd1.collect()
    println(rdd1.isCheckpointed)
    println(rdd1.getCheckpointFile)

  }
}
