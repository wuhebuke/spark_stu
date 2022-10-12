package cn.demo.exam

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{DoubleType, FloatType, LongType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.text.SimpleDateFormat

object ExamTaoBao {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("exam")
    val sparkApp: SparkSession = SparkSession.builder().config(conf)
      .config("hive.metastore.uris","thrift://single03:9083")
      .enableHiveSupport().getOrCreate()
    val sc: SparkContext = sparkApp.sparkContext
    import sparkApp.implicits._
    import org.apache.spark.sql.functions._

/*------------------------------------------RDD------------------------------------------*/
/*    val rdd: RDD[String] = sc.textFile("hdfs://single03:9000/data/userbehavior/UserBehavior.csv")

    //uv 用户访问量
    val uv: Long = rdd.map(x => x.split(",")).map(x => x(0)).distinct().count()
    val uv1: Long = rdd.map(x => x.split(",")).map(x => x(0)).groupBy(x=>x).count()

    //pv 点击量
    val pv: Long = rdd.map(x => x.split(",")).filter(x=>x(3)=="pv").count()
    //fav 收藏量
    val fav: Long = rdd.map(x => x.split(",")).filter(x=>x(3)=="fav").count()
    //buy 购买量
    val buy: Long = rdd.map(x => x.split(",")).filter(x=>x(3)=="buy").count()
    //cart 加入购物车数量
    val cart: Long = rdd.map(x => x.split(",")).filter(x=>x(3)=="cart").count()

    val num1: Long = rdd.map(x => x.split(",")).map(x=>(x(3),1).reduceByKey(_+_).foreach(println)
    val num1: Long = rdd.map(x => x.split(",")).map(x=>(x(3),x(0)).groupByKey().map(x=>(x._1,x._2.toList.size)).collect.foreach(println)

    */

/*------------------------------------------spark sql------------------------------------------*/
//    val df: DataFrame = sparkApp.read.format("csv")
//      .load("hdfs://single03:9000/data/userbehavior/UserBehavior.csv")
//      .toDF("user_id", "item_id", "category_id", "behavior_type", "time")

   /* sparkApp.sql("""
                select t.user_id,
                          (case when t.diff between 0 and 6 then 4
                               when t.diff between 7 and 12 then 3
                               when t.diff between 13 and 18 then 2
                               when t.diff between 19 and 24 then 1
                               else 0
                               end) level
                   from
                       (select user_id,max(`date`) maxNum,datediff("2017-12-03",max(`date`)) diff
                       from exam.userbehavior_partitioned where behavior_type="buy" and `date`>"2017-11-03" group by user_id) t
                       """)
      .show()*/

    /*sparkApp.sql("""
                select
                t.user_id,
                   (case when t.num between 1 and 32 then 0
                    when t.num between 33 and 64 then 1
                    when t.num between 65 and 96 then 2
                    when t.num between 97 and 128 then 3
                    when t.num between 129 and 161 then 4
                    else 0 end)  level
                from
                (select user_id,count(user_id) num
                from userbehavior_partitioned where behavior_type="buy" and `date`>"2017-11-03" group by user_id) t
                       """)
      .show()*/

  }
}
