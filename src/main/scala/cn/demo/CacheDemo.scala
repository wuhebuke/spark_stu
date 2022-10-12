package cn.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object CacheDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("cacheDemo")
    val sc:SparkContext= SparkContext.getOrCreate(conf)

    val rdd1: RDD[String] = sc.textFile("input/users.csv")

//    rdd1.cache()  //设置缓存的方法,默认MEMORY_ONLY
    //不是马上执行缓存操作,等到下一次遇到行动算子时才进行缓存操作
    rdd1.persist(StorageLevel.MEMORY_ONLY_2)


    //第一次
    var start: Long = System.currentTimeMillis()
    println(rdd1.count())
    var end: Long = System.currentTimeMillis()
    println("one "+(end-start))
    println("----------")

    val rdd2: RDD[String] = sc.textFile("input/users.csv")
    //第二次
    start=System.currentTimeMillis()
    println(rdd2.count())
    end=System.currentTimeMillis()
    println("two "+(end-start))
    println("----------")

    //第三次
    start=System.currentTimeMillis()
    println(rdd2.count())
    end=System.currentTimeMillis()
    println("three "+(end-start))
    println("----------")

    //第四次
    start=System.currentTimeMillis()
    rdd1.unpersist() //让缓存失效
    println(rdd2.count())
    end=System.currentTimeMillis()
    println("four "+(end-start))

  }

}
