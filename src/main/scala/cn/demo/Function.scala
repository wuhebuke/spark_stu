package cn.demo

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

object Function {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("spark02").setMaster("local[*]")
    val sc: SparkContext = SparkContext.getOrCreate(conf)

    val data: RDD[Int] = sc.parallelize(1 to 10)
  //    data.collect.foreach(println)
  //    data.filter(x=>x%2==0).foreach(println
  //    data.flatMap(x=>x to 20).collect().foreach(println)

    val data1 = sc.parallelize(Array(Array(1, 2, 4), Array(4, 5, 6)))
  //    data1.collect(println)
  //    data1.flatMap(x=>x).collect()

    val data2 = sc.parallelize(Array(Array(Array(1, 2), Array(3, 5), Array(5, 6), Array(7, 9))))
  //    data2.flatMap(x=>x).flatMap(x=>x).collect()

    val data3=sc.parallelize(Array("hello","java","spark","html"))
  //    data3.map(x=>(x,1))
  //    data3.map(x=>(x,x.length))
  //    data3.mapPartitions(x=>{for(e<-x) yield (e,e.length)})

    /*val num =10
    val ints: immutable.IndexedSeq[Int] = for (i <-  1 to num;if i % 2 == 0) yield i
    val it: Iterator[Int] = ints.iterator
    while (it.hasNext){
      println(it.next())
    }*/

    val data4: RDD[String] = sc.makeRDD(Array("hello", "java", "spark", "scala", "html", "jdbc", "flink", "kfaka"), 2)
   //    data4.map(x=>(x,x.length)).collect
  //    data4.mapPartitions(x=>{for(e<-x) yield (e,e.length)}).collect
  //    data4.mapPartitionsWithIndex((n,x)=>{for(e<-x) yield (e,n,e.length)}).collect

    /*val data5=sc.parallelize(1 to 10)
    val data6=sc.parallelize(1 to 5,2)
    data5.intersection(data6).collect
    data5.intersection(data6).glom.collect
    data5.intersection(data6).sortBy(x=>x,true).collect  //false倒序,true正序
    data5.intersection(data6).sortBy(x=>x,true,2).glom.collect
    val data7=sc.parallelize(5 to 15,2)
    data5.union(data7).collect
    data5.union(data7).distinct.collect
    data5.union(data7).distinct.sortBy(x=>x,false).collect*/

    val strRDD=sc.parallelize(Array("java","spark","go","python","r","kafka","hudi","clickhouse"))
    val strRDD1 = sc.parallelize(Array("java","spark","go","r","python","kafka","hudi","clickhouse","doris","python","kafka","hudi","clickhouse","python","kafka"))
    //    strRDD.mapPartitions(x=>{for(e<-x) yield (e,e.length)}).sortBy(x=>x._2,true).collect.foreach(x=>print(x+" "))
    //    strRDD1.map(x=>(x,x.length)).groupBy(x=>x._1).mapValues(_.size).sortBy(x=>x._2).collect.foreach(x=>println(x))
    //    strRDD1.map(x=>(x,x.length)).reduceByKey((x,y)=>x+y).sortBy(x=>x._2).collect.foreach(x=>println(x))
    //    strRDD1.map(x => (x, x.length)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    //    strRDD1.map(x=>(x,x.length)).sortBy(x=>x._2).foreach(x=>print(x))

  }
}
