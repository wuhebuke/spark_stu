package cn.demo.stream

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util

/**
 * 将数据从kafka的一个topic中取出来，处理加工，再将加工处理过的数据输出到另一个kafka中
*/
object SparkStreamKafkaSourceToSink {
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
    val wordCount: DStream[(String, Int)] = kafkaStream.flatMap(x => x.value().split(" ")).map(x => (x, 1)).reduceByKey(_ + _)

    wordCount.foreachRDD(
      rdd=>{
        rdd.foreachPartition(   //例如 rdd = {[1,2,3],[4,5,6],[7,8]} 三个分区，此代码会在不同的分区内创建三个producer
          x=>{
            val props = new util.HashMap[String, Object]()
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"single03:9092")
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

            val producer = new KafkaProducer[String, String](props)

            x.foreach(
              item=>{
                val record = new ProducerRecord[String, String]("sparkKafkaTopicIn",
                  item._1 + " show times " + item._2)
                producer.send(record)
              }
            )
          }
        )

        /*
        //每取一个数据就会执行一次下面的语句产生一个生产者，效率低；若数据庞大可能会导致挂机
        rdd.foreach(   //例如 rdd = {[1,2,3],[4,5,6],[7,8]} 此rdd有8个元素，每个元素都会创建一个producer
          y=>{
            val props = new util.HashMap[String, Object]()
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"single03:9092")
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")

            val producer = new KafkaProducer[String, String](props)

            val record = new ProducerRecord[String, String]("sparkKafkaTopicIn", y._1 + " show times " + y._2)
            producer.send(record)
          }
        )*/
      }
    )

    streamContext.start()
    streamContext.awaitTermination()

  }

}
