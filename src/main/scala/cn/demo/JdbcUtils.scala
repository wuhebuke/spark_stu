package cn.demo

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.util.Properties

object JdbcUtils {
  val driver="com.mysql.jdbc.Driver"
  val url="jdbc:mysql://single03:3306/sparkEtl"
  val user="root"
  val password="kb18"

  val prop=new Properties()
  prop.setProperty("driver",driver)
  prop.setProperty("user",user)
  prop.setProperty("password",password)

  def getDataFrameByTableName(spark:SparkSession,table:String):DataFrame={
    spark.read.jdbc(JdbcUtils.url,table,prop)
  }

  def saveDataToMysql(df:DataFrame,table:String):Unit={
    df.write.mode(SaveMode.Append).jdbc(JdbcUtils.url, table, prop)
    println("save to mysql [succeed]")
  }


}
