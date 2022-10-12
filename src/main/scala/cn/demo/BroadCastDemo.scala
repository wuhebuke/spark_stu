package cn.demo

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object BroadCastDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("cacheDemo")
    val sc:SparkContext= SparkContext.getOrCreate(conf)


    //累加器
    val myAccumulator: LongAccumulator = sc.longAccumulator("MyAccumulator")
    val rdd: RDD[Int] = sc.parallelize(1 to 10,3)
    val size: Int = rdd.partitions.size
    println(size)
    rdd.glom().collect().foreach(x=>println(x.toList))
    var myCount=1
    val rdd2: RDD[(Int, Int)] = rdd.map(x => {
      println("Accumulator value is " + myAccumulator + ",myCount value is " + myCount + ",x value is " + x)
      myCount += 1
      myAccumulator.add(x)
      (x, 1)
    })
    rdd2.collect().foreach(println)
    println("Result: myAccumulator value is "+myAccumulator+",myCount value is "+myCount)


    /*val arr = Array("hello", "hi", "come on baby")
    //将arr定义为广播变量,会提高后续执行效率
    val broadCastVar: Broadcast[Array[String]] = sc.broadcast(arr)

    val rdd: RDD[(Int, String)] = sc.parallelize(Array((1, "leader"), (2, "teamLeader"), (3, "worker")))
    val rdd2: RDD[(Int, String)] = rdd.mapValues(x => {
      println("value is " + x)
//      broadCastVar.value(0) + ": " + x
      x+" say: "+broadCastVar.value(2)
//      arr(0)+": "+x   此代码效果同上:broadCastVar.value(0) + ": " + x,但其运行效率低于broad
    })
    rdd2.collect().foreach(println)*/


  }
}
