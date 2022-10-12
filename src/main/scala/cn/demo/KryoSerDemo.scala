package cn.demo

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object KryoSerDemo {
  case class NB(id: Integer, name: String)

  case class KB(id: Integer, name: String)

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("kryo")
      .set("spark.serializer", "oeg.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[NB]))

    val sparkApp: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    val df: DataFrame = sparkApp.createDataFrame(Seq(NB(1, "anDeMen"), NB(2, "wneDing"))).toDF()

    val start: Long = System.currentTimeMillis()
    df.persist(StorageLevel.MEMORY_ONLY_SER)

    df.collect().foreach(println)
    val end: Long = System.currentTimeMillis()
    println("time " + (end - start))

  }

}
