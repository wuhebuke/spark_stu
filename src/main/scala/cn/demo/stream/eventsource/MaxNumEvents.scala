package cn.demo.stream.eventsource

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object MaxNumEvents {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkStream")
    val sc = new StreamingContext(conf, Seconds(3))

    sc.checkpoint("checkPoint")

    val kafkaParams: Map[String, String] = Map(
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.42.122:9092"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG ->"earliest"),
      (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG ->"false"),
      (ConsumerConfig.GROUP_ID_CONFIG, "EARToEA")
    )

    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      sc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(Set("event_attendees_raw"), kafkaParams)
    )

    val value = kafkaStream.map(events => {
      val fields: Array[String] = events.value().split(",")
      if (fields.length==5)
      (fields(0), fields(3).split(" ").length)
      else
        (fields(0),0)
    }).reduceByKeyAndWindow((x:Int, y:Int) => {
      if (x > y) x else y
    }, Seconds(9), Seconds(3))

    value.print()

    sc.start()
    sc.awaitTermination()

  }
}
