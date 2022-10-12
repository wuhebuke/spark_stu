package cn.demo.stream

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkStreamDemo2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkScalaStream")
    val streamContext = new StreamingContext(conf, Seconds(5))

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
    val mapStream: DStream[(String, Int)] = kafkaStream.flatMap(x => x.value().split(" ")).map(x => (x, 1))

    //无状态
    //val rst: DStream[(String, Int)] = mapStream.reduceByKey(_ + _)
    //rst.print()

    //记录每一次统计出来的数据
    val stateStream: DStream[(String, Int)] = mapStream.updateStateByKey {
      case (seq, buffer) => {
        println("seq value: "+ seq.toList.toString())
        println("buffer "+ buffer.getOrElse(0).toString)
        val sum = buffer.getOrElse(0) + seq.sum
        Option(sum)
      }
    }

    stateStream.print()

    streamContext.start()
    streamContext.awaitTermination()
  }

}
