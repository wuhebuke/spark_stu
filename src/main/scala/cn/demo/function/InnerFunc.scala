package cn.demo.function

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object InnerFunc {
  def main(args: Array[String]): Unit = {
    val accessLog = Array(
      "2016-12-27,001",
      "2016-12-27,001",
      "2016-12-27,002",
      "2016-12-28,003",
      "2016-12-28,004",
      "2016-12-28,002",
      "2016-12-28,002",
      "2016-12-28,001"
    )
    val conf: SparkConf = new SparkConf().setAppName("innerFunc").setMaster("local[*]")
    val sparkApp: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = sparkApp.sparkContext
    import sparkApp.implicits._

    val rdd1: RDD[String] = sc.parallelize(accessLog)
    val rdd2: RDD[Row] = rdd1.map(x => {
      val field: Array[String] = x.split(",")
      Row(field(0), field(1).toInt)
    })
//    rdd2.collect().foreach(println)

    val schema: StructType = StructType(
      Array(
        StructField("day", StringType),
        StructField("userID", IntegerType)
      ))
    val df: DataFrame = sparkApp.createDataFrame(rdd2, schema)
//    df.show()

    //pv用户访问量 uv使用次数
    import org.apache.spark.sql.functions._
    df.groupBy("day").agg(count("userID").as("pv")).show()
    df.groupBy("day").agg(countDistinct("userID").as("uv")).show()

    //第二种方式创建df
    val rdd3: RDD[(String, Int)] = rdd1.map(x => {
      val field: Array[String] = x.split(",")
      (field(0), field(1).toInt)
    })

    val df2: DataFrame = rdd3.toDF("day", "userID")
    df2.printSchema()
    df2.show()



  }

}
