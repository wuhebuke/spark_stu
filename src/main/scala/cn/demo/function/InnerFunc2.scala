package cn.demo.function

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object InnerFunc2 {
  case class Student(id:Integer,name:String, gender:String,age:Integer)

  def main(args: Array[String]): Unit = {
    val students: Seq[Student] = Seq(
      Student(1, "zhangsan", "F", 22),
      Student(2, "lisi", "M", 38),
      Student(3, "wangwu", "M", 13),
      Student(4, "zhaoliu", "F", 17),
      Student(5, "songba", "M", 32),
      Student(6, "sunjiu", "M", 16),
      Student(7, "qianshiyi", "F", 17),
      Student(8, "yinshier", "F", 15),
      Student(9, "fangshisan", "M", 12),
      Student(10, "yeshisan", "F", 11),
      Student(11, "ruishiyi", "F", 26),
      Student(12, "chenshier", "M", 28)
    )
    val conf: SparkConf = new SparkConf().setAppName("innerFunc").setMaster("local[*]")
    val sparkApp: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import sparkApp.implicits._
    import org.apache.spark.sql.functions._

    val student = students.toDF()

    //所有人的平均年龄 API
//    student.agg(avg("age")).show()
    //分组求平均值
//    student.groupBy("gender").agg(avg("age").as("avg")).show()
//    student.groupBy("gender").agg(avg("age").as("avg"),max("age").as("maxAge"),min("age").as("minAge")).show()
    student.groupBy("gender").agg("age"->"avg","age"->"max","age"->"min","age"->"count").sort($"avg(age)".asc).show()


  }

}
