package cn.demo.function

import cn.demo.function.InnerFunc2.Student
import org.apache.derby.impl.sql.execute.UserDefinedAggregator
import org.apache.parquet.filter2.predicate.Operators.UserDefined
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object UDAF {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("definedFunction").setMaster("local[*]")
    val sparkApp: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = sparkApp.sparkContext
    import sparkApp.implicits._
    import org.apache.spark.sql.functions._

    val students: Seq[Student] = Seq(
      Student(1, "zhangsan", "F", 22),
      Student(2, "lisi", "M", 38),
      Student(3, "wangwu", "M", 13),
      Student(4, "zhaoliu", "F", 17),
      Student(5, "songba", "M", 32),
      Student(6, "sunjiu", "M", 16),
      Student(7, "qianshiyi", "F", 17),
      Student(8, "yinshier", "F", 15),
      Student(9, "fangshisan", "M", 12),
      Student(10, "yeshisan", "F", 11),
      Student(11, "ruishiyi", "F", 26),
      Student(12, "chenshier", "M", 28)
    )
    val df: DataFrame = students.toDF()
    df.createOrReplaceTempView("students")

    //多进一出 UDAF
    sparkApp.udf.register("avgAge",new AvgAgeFunc)
    sparkApp.udf.register("maxAge",new MaxAgeFunc)

    val rstDF: DataFrame = sparkApp.sql("select gender,avg(age) as avg from students group by gender")
    val rstDF1: DataFrame = sparkApp.sql("select gender,avgAge(age) as avgAge from students group by gender")
    val rstDF2: DataFrame = sparkApp.sql("select gender,maxAge(age) as maxAge from students group by gender")
//    rstDF.printSchema()
//    rstDF1.show()
    rstDF2.show()
  }
}

class AvgAgeFunc extends UserDefinedAggregateFunction{
  //聚合函数的输入数据结构
  override def inputSchema: StructType ={
    new StructType().add("age",LongType)
  }

  //缓存区内数据结构
  override def bufferSchema: StructType = {
    new StructType().add("sum",LongType).add("count",LongType)
  }

  //聚合函数的返回值的数据结构
  override def dataType: DataType = {
    DoubleType
  }

  //聚合函数,相同的输入是否总是要得到相同的输出 当前函数是否幂等
  override def deterministic: Boolean = true

  //初始化的值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L  //初始 sum=0
    buffer(1) = 0L  //初始 count=0
  }

  //每传入一条新的数据,要进行的操作:将新的数据的age累加到缓存中,同时缓冲计算器的count+1
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0)+input.getLong(0)  //sum = sum + newAge
    buffer(1) = buffer.getLong(1)+1  //count = count +1
  }

  //分区间数据合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0)+buffer2.getLong(0)  //sum = part1Sum + part2Sum
    buffer1(1) = buffer1.getLong(1)+buffer2.getLong(1)  //count = part1Count + part2Count
  }

  //计算最终结果
  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0).toDouble/buffer.getLong(1)   //avg = sum/count
  }
}

class MaxAgeFunc extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = {
    new StructType().add("age",LongType)
  }

  override def bufferSchema: StructType = {
    new StructType().add("max",LongType)
  }

  override def dataType: DataType = {
    LongType
  }

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=Math.max(buffer.getLong(0),input.getLong(0))

  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = Math.max(buffer1.getLong(0),buffer2.getLong(0))

  }

  override def evaluate(buffer: Row): Any = {
    buffer.getLong(0)
  }
}
