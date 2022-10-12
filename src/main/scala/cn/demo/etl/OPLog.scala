package cn.demo.etl

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object OPLog {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("etlDemo").setMaster("local[*]")
    val sparkApp: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = sparkApp.sparkContext
    import org.apache.spark.sql.functions._
    import sparkApp.implicits._

    val rdd: RDD[String] = sc.textFile("input/op.log")
    //    rdd.collect().foreach(println)
    val option: RDD[(String, String)] = rdd.map(x => {
      val fields: Array[String] = x.split("\\|")
      (fields(0), fields(1))
    })

    //    val jsonStrRDD: RDD[String] = option.map(_._2)
    //    val jsonDF: DataFrame = jsonStrRDD.toDF()

    val jsonStrRDD1: RDD[(String, String)] = option.map(x => (x._1, x._2))
    val jsonStrDF: DataFrame = jsonStrRDD1.toDF("userid", "value")

    val jsonObj: DataFrame = jsonStrDF.select(
      $"userid",
      get_json_object($"value", "$.cm").as("cm"),
      get_json_object($"value", "$.ap").as("ap"),
      get_json_object($"value", "$.et").as("et")
    )

    val jsonObj2: DataFrame = jsonObj.select($"userid", $"ap",
      get_json_object($"cm", "$.ln").as("ln"),
      get_json_object($"cm", "$.sv").as("sv"),
      get_json_object($"cm", "$.os").as("os"),
      get_json_object($"cm", "$.g").as("g"),
      get_json_object($"cm", "$.mid").as("mid"),
      get_json_object($"cm", "$.nw").as("nw"),
      get_json_object($"cm", "$.l").as("l"),
      get_json_object($"cm", "$.vc").as("vc"),
      get_json_object($"cm", "$.hw").as("hw"),
      get_json_object($"cm", "$.ar").as("ar"),
      get_json_object($"cm", "$.uid").as("uid"),
      get_json_object($"cm", "$.t").as("t"),
      get_json_object($"cm", "$.la").as("la"),
      get_json_object($"cm", "$.md").as("md"),
      get_json_object($"cm", "$.vn").as("vn"),
      get_json_object($"cm", "$.ba").as("ba"),
      get_json_object($"cm", "$.sr").as("sr"),
      $"et"
    )
    val jsonDF2: DataFrame = jsonObj2.select($"et")

    val schema: ArrayType = ArrayType(
      StructType(
        StructField("ett", StringType) ::
          StructField("en", StringType) ::
          StructField("kv", StringType) :: Nil
      ))

    val jsonObj3: DataFrame = jsonObj2.select(
      $"userid", $"ap",
      $"ln", $"sv", $"os", $"g", $"mid", $"nw", $"l", $"vc", $"hw", $"ar", $"uid", $"t", $"la", $"md", $"vn", $"ba", $"sr",
      from_json($"et", schema).as("events"))
    jsonObj3.show(false)

    val jsonObj4: DataFrame = jsonObj3.withColumn("events", explode($"events"))
      .select($"userid", $"ap",
        $"ln", $"sv", $"os", $"g", $"mid", $"nw", $"l", $"vc", $"hw", $"ar", $"uid", $"t", $"la", $"md", $"vn", $"ba", $"sr", $"events")
    //    jsonObj4.printSchema()
    //    jsonObj4.show(false)

    val jsonObj5: DataFrame = jsonObj4.select(
      $"userid", $"ap",
      $"ln", $"sv", $"os", $"g", $"mid", $"nw", $"l", $"vc", $"hw", $"ar", $"uid", $"t", $"la", $"md", $"vn", $"ba", $"sr",
      $"events.ett", $"events.en", $"events.kv"
    )
    //    jsonObj5.show()

    val loadingDS: Dataset[Row] = jsonObj5.filter($"en" === "loading")
    val loadingDF: DataFrame = loadingDS.select(
      $"userid", $"ap",
      $"ln", $"sv", $"os", $"g", $"mid", $"nw", $"l", $"vc", $"hw", $"ar", $"uid", $"t", $"la", $"md", $"vn", $"ba", $"sr",
      $"ett", $"en",
      get_json_object($"kv", "$.extend2").as("extend2"),
      get_json_object($"kv", "$.loading_time").as("loading_time"),
      get_json_object($"kv", "$.action").as("action"),
      get_json_object($"kv", "$.extend1").as("extend1"),
      get_json_object($"kv", "$.type").as("type"),
      get_json_object($"kv", "$.type1").as("type1"),
      get_json_object($"kv", "$.loading_way").as("loading_way")
    )
    //    loadingDF.show()

    val adDS: Dataset[Row] = jsonObj5.filter($"en" === "ad")
    val adDF: DataFrame = adDS.select(
      $"userid", $"ap",
      $"ln", $"sv", $"os", $"g", $"mid", $"nw", $"l", $"vc", $"hw", $"ar", $"uid", $"t", $"la", $"md", $"vn", $"ba", $"sr",
      $"ett", $"en",
      get_json_object($"kv", "$.activityId").as("activityId"),
      get_json_object($"kv", "$.displayMills").as("displayMills"),
      get_json_object($"kv", "$.entry").as("entry"),
      get_json_object($"kv", "$.action").as("action"),
      get_json_object($"kv", "$.contentType").as("contentType")
    )
    //    adDF.show()

    val notificationDS: Dataset[Row] = jsonObj5.filter($"en" === "notification")
    val notificationDF: DataFrame = notificationDS.select(
      $"userid", $"ap",
      $"ln", $"sv", $"os", $"g", $"mid", $"nw", $"l", $"vc", $"hw", $"ar", $"uid", $"t", $"la", $"md", $"vn", $"ba", $"sr",
      $"ett", $"en",
      get_json_object($"kv", "$.ap_time").as("ap_time"),
      get_json_object($"kv", "$.action").as("action"),
      get_json_object($"kv", "$.type").as("type"),
      get_json_object($"kv", "$.content").as("content")
    )
    //    notificationDF.show()

    val active_backgroundDS: Dataset[Row] = jsonObj5.filter($"en" === "active_background")
    val active_backgroundDF: DataFrame = active_backgroundDS.select(
      $"userid", $"ap",
      $"ln", $"sv", $"os", $"g", $"mid", $"nw", $"l", $"vc", $"hw", $"ar", $"uid", $"t", $"la", $"md", $"vn", $"ba", $"sr",
      $"ett", $"en",
      get_json_object($"kv", "$.active_source").as("active_source")
    )
    //    active_backgroundDF.show()

    val commentDS: Dataset[Row] = jsonObj5.filter($"en" === "comment")
    val commentDF: DataFrame = commentDS.select(
      $"userid", $"ap",
      $"ln", $"sv", $"os", $"g", $"mid", $"nw", $"l", $"vc", $"hw", $"ar", $"uid", $"t", $"la", $"md", $"vn", $"ba", $"sr",
      $"ett", $"en",
      get_json_object($"kv", "$.p_comment_id").as("p_comment_id"),
      get_json_object($"kv", "$.addtime").as("addtime"),
      get_json_object($"kv", "$.praise_count").as("active_source"),
      get_json_object($"kv", "$.other_id").as("other_id"),
      get_json_object($"kv", "$.comment_id").as("comment_id"),
      get_json_object($"kv", "$.reply_count").as("reply_count"),
      get_json_object($"kv", "$.userid").as("userid"),
      get_json_object($"kv", "$.content").as("content"),
    )
    //    commentDF.show()

  }
}
