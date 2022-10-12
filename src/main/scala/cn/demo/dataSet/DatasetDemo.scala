package cn.demo.dataSet

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DatasetDemo {

  case class Point (label:String,x:Double,y:Double,tag1:String)
  case class School(label:String,address:String,tag2:String)
  case class Category(id:Long,name:String)


  def main(args: Array[String]): Unit = {
    val sparkApp: SparkSession = SparkSession.builder()
      .master("local[*]").appName("dsDemo")
      .config("spark.sql.crossJoin.enabled", true).getOrCreate()

    import sparkApp.implicits._  //引入隐式方法时使用的是sparkSession对象名
    val sc: SparkContext = sparkApp.sparkContext


//    val ds1: Dataset[Int] = sparkApp.createDataset(1 to 6)
//    ds1.show()
    //Dataset任意类型,Dataframe只能是row类型

    val ds2: Dataset[(String, Int)] = sparkApp.createDataset(List(("a", 1), ("b", 2), ("c", 3)))
      //修改表头名字
//    ds2.withColumnRenamed("_1","name")
//      .withColumnRenamed("_2","age").show()

    val ds3: Dataset[(String, Int,Int)] = sparkApp.createDataset(sc.parallelize(List(("frank", 23,173), ("jack", 34,165), ("jerry", 28,140))))
//    ds3.printSchema()
//    ds3.show()
//    ds3.withColumnRenamed("_1","name")
//      .withColumnRenamed("_2","age")
//      .withColumnRenamed("_3","height").show()

//    ds3.select($"_1",$"_2").show()
//    ds3.select("_1","_2").show()
//    ds3.withColumnRenamed("_1","name")
//      .withColumnRenamed("_2","age")
//      .withColumnRenamed("_3","height")
//      .where($"age">25)
//      .select($"name",$"age")
//      .show()


    val points: Dataset[Point] = Seq(Point("frank", 23, 31,"1"), Point("jack", 12, 13,"1"), Point("jerry", 34, 23,"2"), Point("wei", 23, 12,"2")).toDS()
    val category: Dataset[Category] = Seq(Category(1, "frank"), Category(2, "jack"),Category(4,"white")).toDS()
    val schools: Dataset[School] = Seq(School("frank", "adm","1"), School("jack", "yuHuaTai","2")).toDS()

    //select * from point join category on point.label=category.name
//    val df: DataFrame = points.join(category, points("label") === category("name"))
//    val df1: DataFrame = points.join(category, $"label" === $"name", "right")
//    val df2: DataFrame = points.join(schools, $"label" === $"label")  //报错 两个参数相似(ambiguous)无法识别
//    val df3: DataFrame = points.join(schools, points("label") === schools("label"))  //会出现相同表头,label
    val df4: DataFrame = points.join(schools, Seq("label"))  //只有一个label表头
    df4.show()
//    val df5: DataFrame = points.withColumnRenamed("tag", "tag1").join(schools, Seq("label"))
//    df5.printSchema()
//    df5.show()


    //select * from A join B on A.a=B.b and A.b=B.b
//    val df6: DataFrame = points.join(schools, Seq("label", "tag"))

//    val df7: DataFrame = points.join(schools, $"label1" === $"label2" and $"tag1"===$"tag2")
//    df7.show()





  }

}
