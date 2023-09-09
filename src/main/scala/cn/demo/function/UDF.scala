package cn.demo.function

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object UDF {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("definedFunction").setMaster("local[*]")
    val sparkApp: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = sparkApp.sparkContext
    import sparkApp.implicits._

    val rdd: RDD[String] = sc.textFile("input/hobbies.txt")
    val df: DataFrame = rdd.map(_.split(" ")).map(x => (x(0), x(1))).toDF("name","hobbies")

    df.createOrReplaceTempView("hobby")
    sparkApp.udf.register("hobby_num",(x:String)=>x.split(",").size)
//    sparkApp.sql("select name,hobbies,hobby_num(hobbies) as hobbyNum from hobby").show()

    sparkApp.udf.register("concat2",(x:String,y:String)=>x+" like "+y)
    sparkApp.sql("select name,hobbies,hobby_num(hobbies) as hobbiesNum,concat2(name,hobbies) as concat2 from hobby").show(false)
  }

}
