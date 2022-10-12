package cn.demo.dataFrame

import cn.demo.JdbcUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object DataFrameToMysql {
  def main(args: Array[String]): Unit = {
    val sparkApp: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("sparkToMysql").getOrCreate()
    val sc = sparkApp.sparkContext

    import org.apache.spark.sql._
    import sparkApp.implicits._

    val prop=new Properties()
    prop.setProperty("driver",JdbcUtils.driver)
    prop.setProperty("user",JdbcUtils.user)
    prop.setProperty("password",JdbcUtils.password)


    val student: DataFrame = sparkApp.read.jdbc(JdbcUtils.url, "student", prop)
//    student.show()

//    val rdd: RDD[String] = sc.textFile("input/hello.txt")
//    rdd.collect().foreach(println)
  }

}
