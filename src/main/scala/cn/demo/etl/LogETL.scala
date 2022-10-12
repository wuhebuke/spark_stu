package cn.demo.etl

import cn.demo.JdbcUtils
import cn.demo.JdbcUtils.prop
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object LogETL {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("etlDemo")
    val sparkApp: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = sparkApp.sparkContext
    import sparkApp.implicits._

    val option: RDD[String] = sc.textFile("D:/datasource/log/test.log")
    val rowRDD: RDD[Row] = option
      .map(x => x.split("\t"))
      .filter(_.length == 8)
      .map(x => Row(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7)))

    val logs_schema: StructType = StructType(
      Array(
        StructField("events_time", StringType),
        StructField("url", StringType),
        StructField("method", StringType),
        StructField("status", StringType),
        StructField("sip", StringType),
        StructField("user_uip", StringType),
        StructField("action_prepend", StringType),
        StructField("action_client", StringType)
      )
    )

    /*val schema: ArrayType = ArrayType(
      StructType(
        StructField("events_time", StringType) ::
          StructField("url", StringType) ::
          StructField("method", StringType) ::
          StructField("status", StringType) ::
          StructField("sip", StringType) ::
          StructField("user_uip", StringType) ::
          StructField("action_prepend", StringType) ::
          StructField("action_client", StringType) :: Nil
      )
    )*/

    val logDF: DataFrame = sparkApp.createDataFrame(rowRDD, logs_schema)
    //    logDF.printSchema()
    //    logDF.show()

    val filterLogs: Dataset[Row] = logDF
      .dropDuplicates("events_time", "url") //数据去重
      .filter($"status" === 200)
      .filter($"events_time".isNotNull)
    //      .filter(x=>StringUtils.isNotEmpty(x(0).toString))


    val full_logDF: DataFrame = filterLogs.map(line => {
      //获取 <url> 列的信息
      val str: String = line.getAs[String]("url")
      //以'?' 分割 'url' 列中的信息
      val parArray: Array[String] = str.split("\\?")
      var maps: Map[String, String] = null
      //若 'url' 列中信息有效
      if (parArray.length == 2) {
        //以'&'分割'url'中'?'后的信息
        maps = parArray(1).split("&")
          //以'&'分割'url'中'='后的信息
          .map(x => x.split("="))
          .filter(_.length == 2)
          .map(x => (x(0), x(1))).toMap
      }
      (
        line.getAs[String]("events_time"),
        maps.getOrElse[String]("userUID", ""),
        maps.getOrElse[String]("userSID", ""),
        maps.getOrElse[String]("actionBegin", ""),
        maps.getOrElse[String]("actionEnd", ""),
        maps.getOrElse[String]("actionType", ""),
        maps.getOrElse[String]("actionName", ""),
        maps.getOrElse[String]("actionValue", ""),
        maps.getOrElse[String]("actionTest", ""),
        maps.getOrElse[String]("ifEquipment", ""),
        line.getAs[String]("method"),
        line.getAs[String]("status"),
        line.getAs[String]("sip"),
        line.getAs[String]("user_uip"),
        line.getAs[String]("action_prepend"),
        line.getAs[String]("action_client")
      )
    }).toDF("events_time"
      , "userUID", "userSID", "actionBegin", "actionEnd", "actionType", "actionName", "actionValue"
      , "actionTest", "ifEquipment", "method", "status", "sip", "user_uip", "action_prepend", "action_client")

       /* full_logDF.printSchema()
        full_logDF.show()

        //连接mysql数据库,将数据装载到mysql中
        val prop: Properties = new Properties()
        prop.setProperty("driver",JdbcUtils.driver)
        prop.setProperty("user",JdbcUtils.user)
        prop.setProperty("password",JdbcUtils.password)
        full_logDF.write.mode(SaveMode.Overwrite).jdbc(JdbcUtils.url,"full_log",prop)
        sparkApp.stop()*/


  }
}
