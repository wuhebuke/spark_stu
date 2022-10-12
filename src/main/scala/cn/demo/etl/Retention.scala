package cn.demo.etl

import cn.demo.JdbcUtils
import cn.demo.JdbcUtils.prop
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.streaming.OutputMode.Append
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.text.SimpleDateFormat

object Retention {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("etlDemo").setMaster("local[*]")
    val sparkApp = SparkSession.builder().config(conf).getOrCreate()

    import sparkApp.implicits._
    import org.apache.spark.sql.functions._

    val frame: DataFrame = JdbcUtils.getDataFrameByTableName(sparkApp, "full_log")
//    frame.printSchema()
//    frame.show()

    val register: DataFrame = frame.filter($"actionName" === "Registered")
      .withColumnRenamed("events_time", "register_time")
      .select("userUID", "register_time")
//    register.show()

    val signin: DataFrame = frame.filter($"actionName" === "Signin")
      .withColumnRenamed("events_time", "signin_time")
      .select("userUID", "signin_time")
//    signin.show()
//    println(signin.count())

    val joined: DataFrame = register.join(signin, Seq("userUID"), "left")
//    joined.printSchema()
//    joined.show()

    /*字符串转换时间戳 long类型
    val str: String = "2018-09-04T20:34:45+08:00".substring(0, 10)
    println(str)

    val spdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val time: Long = spdf.parse("2018-09-04").getTime
    println(time)

    val spdf2: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val time1: Long = spdf2.parse("2018-09-04 20:34:45").getTime
    println(time1)*/

    val spdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val gszh: UserDefinedFunction = sparkApp.udf.register("gszh", (events_time: String) => {
      if (StringUtils.isEmpty(events_time))
        0
      else {
        spdf.parse(events_time).getTime
      }
    })

    val joined2: DataFrame = joined.withColumn("register_date", gszh($"register_time"))
      .withColumn("signin_date", gszh($"signin_time"))
//    joined2.show()

    //次日留存数
    val signinNum: DataFrame = joined2.filter($"register_date" + 86400000 === $"signin_date")
      .groupBy($"register_date")
      .agg(countDistinct("userUID").as("signinNum"))
//    signinNum.show()

    //一周留存数
    val signinNum1: DataFrame = joined2.filter($"register_date" + 604800000 <= $"signin_date")
      .groupBy($"register_date")
      .agg(countDistinct("userUID").as("singinNum1"))

    val registerNum: DataFrame = joined2.groupBy("register_date")
      .agg(countDistinct("userUID").as("registerNum"))
//    registerNum.show()

    val joinRegisSignin: DataFrame = signinNum.join(registerNum, Seq("register_date"))
//    joinRegisSignin.show()


    val rst: DataFrame = joinRegisSignin.select($"register_date",round($"signinNum"/$"registerNum",2).as("percent"))

//    rst.show()
    JdbcUtils.saveDataToMysql(rst,"retention")



  }
}
