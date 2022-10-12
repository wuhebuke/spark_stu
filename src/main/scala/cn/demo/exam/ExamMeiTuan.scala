package cn.demo.exam

import org.apache.parquet.format.IntType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ExamMeiTuan {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("exam")
    val sparkApp: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = sparkApp.sparkContext
    import sparkApp.implicits._
    import org.apache.spark.sql.functions._
//-------------------------------------------------RDD----------------------------------------------------
    val fileRDD: RDD[String] = sc.textFile("hdfs://single03:9000/app/data/exam/meituan_waimai_meishi.csv")
    //    fileRDD.collect().foreach(println)

    //去除标题栏
    val spuRDD: RDD[Array[String]] = fileRDD.filter(x => x.startsWith("spu_id") == false)
      .map(x => x.split(",", -1))
      .filter(x => x.size == 12)

    //统计每个店铺的商品数量
    val goodsNum1: RDD[(String, Int)] = spuRDD.map(x => (x(2), 1)).reduceByKey(_ + _)
    val goodsNum2: RDD[(String, Int)] = spuRDD.map(x => (x(2), 1)).groupByKey().map(x => (x._1, x._2.size))
//    goodsNum2.foreach(println)

    //店铺销售总额
    val totalSale: RDD[(String, Double)] = spuRDD.map(x => (x(2), x(5).toDouble * x(7).toInt)).reduceByKey(_ + _)
//    totalSale.foreach(println)

    //统计每个店铺销售额最高的前三个商品，输出内容包括店铺名，商品名和销售额，其中销售额为 0 的商品不进行统计计算
    val top3RDD: RDD[(String, List[(String, String, Double)])] = spuRDD.map(x => (x(2), x(4), x(5).toDouble * x(7).toInt))
      .filter(x => x._3 > 0)
      .groupBy(x => x._1)
      .mapValues(x => x.toList.sortBy(item => -item._3).take(3))

    val top3RDD2: RDD[(String, List[(String,  Double)])] = spuRDD.map(x => (x(2), x(4), x(5).toDouble * x(7).toInt))
      .filter(x => x._3 > 0)
      .groupBy(x => x._1)
      .map(x => {
        var shop_name = x._1
        var top3_goods_sale = x._2.toList.sortBy(item => -item._3)
          .take(3)
        var tmpStr: List[(String, Double)] = top3_goods_sale.map(x => (x._2, x._3))
        (shop_name, tmpStr)
      })

    val top3RDD3: RDD[(String, String, Double)] = spuRDD.map(x => (x(2), x(4), x(5).toDouble * x(7).toInt))
      .filter(x => x._3 > 0)
      .groupBy(x => x._1)
      .flatMap(x => {
        val shop_name: String = x._1
        x._2.toList.sortBy(item => -item._3).take(3)
      })
//    top3RDD3.foreach(println)

//------------------------------------------------------spark sql--------------------------------------------------------------

    val spuDF: DataFrame = sparkApp.read.format("csv").option("header", true)
      .load("hdfs://single03:9000/app/data/exam/meituan_waimai_meishi.csv")
    //    spuDF.show(3,false)

    //筛选字段
    val spuDF2: DataFrame = spuDF.select("shop_id", "shop_name", "spu_name", "spu_price", "month_sales")
    //修改spu_price和month_price的字段类型
    val spuDF3: DataFrame = spuDF2.withColumn("spu_price", $"spu_price".cast(DoubleType))
      .withColumn("month_sales", $"month_sales".cast(IntegerType))

    spuDF3.createOrReplaceTempView("spu")
//    sparkApp.sql("select shop_name,count(shop_name) as count from spu group by shop_name").show()
//    sparkApp.sql("select shop_name,sum(spu_price*month_sales) as sum from spu where month_sales>0 group by shop_name").show()
    //统计每个店铺销售额最高的前三个商品，输出内容包括店铺名，商品名和销售额，其 中销售额为 0 的商品不进行统计计算
    sparkApp.sql("select t.shop_name,t.spu_name,t.totalSales from (select shop_name,spu_name,spu_price*month_sales as totalSales,row_number() over(partition by shop_name order by spu_price*month_sales desc) as rn from spu where month_sales>0) t where t.rn<4").show()



  }

}
