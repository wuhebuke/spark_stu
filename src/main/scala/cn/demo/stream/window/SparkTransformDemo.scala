package cn.demo.stream.window

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.text.SimpleDateFormat

object SparkTransformDemo {
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

    val transformDS: DStream[((String, String), Int)] = kafkaStream.transform(
      (rdd, timestamp) => {
        val format = new SimpleDateFormat("yyyyMMdd HH:mm:ss")
        val time = format.format(timestamp.milliseconds)

        /*val value: RDD[((String, String), Int)] = rdd.flatMap(x => {
          x.value().toString.split(" ").map(x => ((x, time), 1))
        })
        value*/

        val value: RDD[((String, String), Int)] = rdd.flatMap(x => {
          x.value().toString.split(" ").map(x => ((x, time), 1))
        }).reduceByKey(_ + _).sortBy(x=>x._2,false)
        value
      }
    )
    transformDS.print()

    streamContext.start()
    streamContext.awaitTermination()
  }

}
