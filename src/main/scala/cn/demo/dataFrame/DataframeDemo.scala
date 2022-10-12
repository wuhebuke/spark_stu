package cn.demo.dataFrame

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.Struct

object DataframeDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("sparkDemo").setMaster("local[*]")
    val sparkApp: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = sparkApp.sparkContext
    import sparkApp.implicits._

   /* val df: DataFrame = sparkApp.read.format("json").json("input/people.json")
//    df.printSchema()
//    df.select($"name",$"Age").show()
//    df.select(df("name")).show()
//    df.select(df("name"),df("Age")).show()
//    df.select(df("name"),df("Age").as("AGE")).where($"AGE">25).show()
//    df.select(df("name"),(df("Age")+1).as("AGE")).filter($"AGE">25).show()

    import org.apache.spark.sql.functions._
    val df1: DataFrame = df.groupBy("age").agg(count("name").as("count"))
    val df2: DataFrame = df.groupBy("age").count()
//    df1.show()
//    df2.show()

    df1.withColumn("number",$"count".cast(StringType)).printSchema()*/


    val peopleRDD: RDD[String] = sc.textFile("input/people.txt")
    val df: DataFrame = peopleRDD.toDF()

    val rdd2: RDD[(Int, String, Int)] = peopleRDD.map(x => {
      val strs: Array[String] = x.split(" ")
      (strs(0).toInt, strs(1).toString, strs(2).toInt)
    })
    val df2: DataFrame = rdd2.toDF()
    val df3: DataFrame = df2.withColumnRenamed("_1", "id")
      .withColumnRenamed("_2", "name")
      .withColumnRenamed("_3", "age")
//    df3.printSchema()
//    df3.show()

    val df4: DataFrame = rdd2.toDF("id", "name", "age")
//    df4.printSchema()
//    df4.show()

    df4.createOrReplaceTempView("people")
    //    df4.createTempView()
    val rstDF: DataFrame = sparkApp.sql("select * from people")
    //将df读入指定目录所在文件夹
    //    rstDF.write.parquet("output/parquet")
    //读取文件
    val readDF: DataFrame = sparkApp.read.parquet("output/parquet")
    readDF.printSchema()
    readDF.show()



    val rowRDD: RDD[Row] = peopleRDD.map(x => x.split(" ")).map(x => Row(x(0).toInt, x(1), x(2).toInt))
    val fields: Array[StructField] = Array(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("age", IntegerType))
    val schema: StructType = StructType(fields)
    val frame: DataFrame = sparkApp.createDataFrame(rowRDD, schema)
//    frame.show()



  }

}
