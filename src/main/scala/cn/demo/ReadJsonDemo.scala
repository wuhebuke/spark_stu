package cn.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ReadJsonDemo {
  def main(args: Array[String]): Unit = {
//    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("cacheDemo")
//    val sc:SparkContext= SparkContext.getOrCreate(conf)

    /*val sc: SparkContext = sparkSession.sparkContext
    val line: RDD[String] = sc.textFile("input/users.json")
    import scala.util.parsing.json.JSON
    val rdd: RDD[Option[Any]] = line.map(x => JSON.parseFull(x))
    rdd.collect().foreach(println)*/

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("sparkSessionDemo").master("local[*]").getOrCreate()


    val userDF: DataFrame = sparkSession.read.format("json").option("header", "false").load("input/users.json")
    val userDF1: DataFrame = sparkSession.read.format("csv").option("header", "false").load("input/users.json")
    userDF.printSchema()
    userDF.show()
//    userDF1.printSchema()
//    userDF1.show(false)

    //将userDF转换为临时表,可以执行sql语句
    userDF.createOrReplaceTempView("tmp")
    sparkSession.sql("select * from tmp").show()

  }

}
