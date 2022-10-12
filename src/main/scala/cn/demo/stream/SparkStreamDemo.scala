package cn.demo.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreamDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkStream")

    //定义流处理上下文，采集周期为3秒    storm -> sparkStreaming -> flink
    val streamingContext = new StreamingContext(conf, Seconds(3))

    //流数据取数据源设置
    val socketLineStream: ReceiverInputDStream[String] =
      streamingContext.socketTextStream("192.168.42.122", 7777)

    //数据处理，加工
    val rst: DStream[(String, Int)] = socketLineStream.flatMap(line => line.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
//    val mapStream: DStream[(String, Int)] = wordStream.map(x => (x, 1))
//    val rst: DStream[(String, Int)] = mapStream.reduceByKey(_ + _)

    //输出
    rst.print()

    //启动采集器
    streamingContext.start()
    streamingContext.awaitTermination()



  }


}
