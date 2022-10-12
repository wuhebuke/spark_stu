package cn.demo.stream.window

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object SparkWindow1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkScalaStream")
    val streamContext = new StreamingContext(conf, Seconds(2))

    streamContext.checkpoint("checkPoint")

    val kafkaParams: Map[String, String] = Map(
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.42.122:9092"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.GROUP_ID_CONFIG, "streamKafkaGroup1")
    )

    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      streamContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(Set("sparkKafkaTopic"), kafkaParams)
    )

    //窗口开窗 window(windowDuration: Duration, slideDuration: Duration) 参数一：窗口大小，参数二：窗口滑动步伐【左闭右开】
    /*val windowStream: DStream[(String, Int)] = kafkaStream.flatMap(line => line.value().split(" ")).map(x => (x, 1))
      .window(Seconds(8), Seconds(2))
    windowStream.print()*/

    /*val windowStream1: DStream[(String, Long)] = kafkaStream.flatMap(line => line.value().split(" "))
      .countByValueAndWindow(Seconds(8), Seconds(6))
    windowStream1.print()*/

    /*val windowStream2: DStream[Long] = kafkaStream.flatMap(line => line.value().split(" "))
      .countByWindow(Seconds(8), Seconds(4))
    windowStream2.print()*/

    /*val windowStream3: DStream[String] = kafkaStream.flatMap(line => line.value().split(" "))
      .reduceByWindow(
        (x, y) => x + "-" + y,
        (x,y)=>x+"+"+y,
        Seconds(8), Seconds(4))
    windowStream3.print()*/

    /*val windowStream4: DStream[(String, Int)] = kafkaStream.flatMap(line => line.value().split(" "))
      .map(x => (x, 1))
      .reduceByWindow((x, y) => ("count", x._2 + y._2), Seconds(8), Seconds(4))
    windowStream4.print()*/

    val windowStream5: DStream[(String, Int)] = kafkaStream.flatMap(line => line.value().split(" ")).map(x => (x, 1))
      .reduceByKeyAndWindow(
        (x:Int, y:Int) => x + y,
        Seconds(8),
        Seconds(4)
      )
    windowStream5.print()

    streamContext.start()
    streamContext.awaitTermination()
  }

}
