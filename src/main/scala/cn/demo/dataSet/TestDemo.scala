package cn.demo.dataSet

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object TestDemo {
  case class Customer(id:String,fname:String,lname:String,email:String,password:String,street:String,city:String,state:String,zipcode:String)
  case class Order(id:String,date:String,customerID:String,status:String)
  case class OrderItem(id:String,orderID:String,productID:String,quantity:Int,subtotal:Float,productPrice:Float)
  case class Product(id:String,categoryID:String,name:String,description:String,price:Float,image:String)

  def main(args: Array[String]): Unit = {
    val sparkApp: SparkSession = SparkSession.builder()
      .master("local[*]").appName("dsDemo")
      .config("spark.sql.crossJoin.enabled", true).getOrCreate()

      val sc: SparkContext = sparkApp.sparkContext
      import sparkApp.implicits._

    //customers.csv
    val customers: Dataset[Customer] = sc.textFile("input/customers.csv").map(x => {
      val field: Array[String] = x.split(",")
      Customer(field(0), field(1), field(2), field(3), field(4), field(5), field(6), field(7), field(8))
    }).toDS()


    //orders.csv
    val orders: Dataset[Order] = sc.textFile("input/orders.csv").map(x => {
      val field: Array[String] = x.split(",")
      Order(field(0), field(1), field(2), field(3))
    }).toDS()


    //orderItems.csv
    val oiLine: RDD[String] = sc.textFile("input/order_items.csv")
    val orderItemsRDD: RDD[OrderItem] = oiLine.map(x => {
      val field: Array[String] = x.split(",")
      OrderItem(field(0), field(1), field(2), field(3).replace("\"","").toInt, field(4).replace("\"","").toFloat, field(5).replace("\"","").toFloat)
    })
    val orderItems: Dataset[OrderItem] = orderItemsRDD.toDS()


    //products.csv
    val proLine: RDD[String] = sc.textFile("input/products.csv")
    val proRDD: RDD[Product] = proLine.map(x => {
      val field: Array[String] = x.split(",")
//      val priceStr: String = field(4).replace("\"", "")
//      var price=0.0f
//      if(priceStr.length!=0)
//        price=priceStr.toFloat
      Product(
        field(0),
        field(1),
        field(2),
        field(3),
        //price
        scala.util.control.Exception.failAsValue[Float](classOf[Exception])(0)(field(4).replace("\"","").toFloat),
        field(5))
    })
    val products: Dataset[Product] = proRDD.toDS()

    //消费额最高的产品
    import org.apache.spark.sql.functions._
    //select sum("subtotal") as total from orders join orderItems on orders.id=orderItems.orderID group by customerID order by total desc
    println("消费额最高的产品")
   /* val ds1: Dataset[Row] = orders.join(orderItems, orders("id") === orderItems("orderID"))
      .groupBy("customerID")
      .agg(sum("subtotal").alias("total"))
      .sort(desc("total"))
    ds1.show(false)*/

    println("⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐⭐")

    //销量最高的产品
    //select productID sum(quantity) as quantity from orderItems join products on orderItems.productID=products.id group by productID
    /*println("销量最高的产品")
    orderItems.groupBy("productID").agg(sum("quantity").alias("quantity")).alias("ods")
      .join(products,products("id")===col("ods.productID"))
      .select("id","name","quantity")
      .sort(desc("quantity"))
      .show(false) //.show(num,false)显示num行全表信息*/


  }

}
