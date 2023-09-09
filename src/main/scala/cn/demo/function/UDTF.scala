package cn.demo.function

import org.apache.hadoop.hive.ql.exec.MapredContext
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, StructObjectInspector}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util
import shapeless.Generic

object UDTF {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("definedFunction").setMaster("local[*]")
    val sparkApp: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = sparkApp.sparkContext
    import sparkApp.implicits._

    val rdd: RDD[String] = sc.textFile("input/UDTF.txt")

    val df: DataFrame = rdd.map(x => {
      val field: Array[String] = x.split("//")
      (field(0), field(1), field(2))
    }).toDF("id", "name", "skill")
//    df.printSchema()
//    df.show(false)

    df.createOrReplaceTempView("udtfTable")

    sparkApp.sql("create temporary function Myudtf as 'cn.demo.function.MyUDTF'")
    //sparkApp.sql("select skill from udtfTable").show(false)
    sparkApp.sql("select id,name,Myudtf(skill) from udtfTable").show()
  }
}

    //UDTF函数 一进多出
    class MyUDTF extends GenericUDTF{

      override def initialize(argOIs: Array[ObjectInspector]): StructObjectInspector = {
        val fieldName = new util.ArrayList[String]()
        fieldName.add("skill")

        val fieldOIS:util.ArrayList[ObjectInspector] = new util.ArrayList[ObjectInspector]()
        fieldOIS.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)

        ObjectInspectorFactory.getStandardStructObjectInspector(fieldName,fieldOIS)

      }

      override def initialize(argOIs: StructObjectInspector): StructObjectInspector = {
        val fieldName = new util.ArrayList[String]()
        fieldName.add("skill")

        val fieldOIS:util.ArrayList[ObjectInspector] = new util.ArrayList[ObjectInspector]()
        fieldOIS.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector)

        ObjectInspectorFactory.getStandardStructObjectInspector(fieldName,fieldOIS)

      }

      override def process(objects: Array[AnyRef]): Unit = { //[Hadoop scala spark hive hbase]
        val strings: Array[String] = objects(0).toString.split(" ") //[Hadoop,scala,spark,hive,hbase]
        for (str<-strings){
          val temp: Array[String] = new Array[String](1)
          temp(0)=str
          forward(temp)
        }
      }

      override def close(): Unit = {

  }
}
