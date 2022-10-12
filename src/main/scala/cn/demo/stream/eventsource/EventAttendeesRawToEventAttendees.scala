package cn.demo.stream.eventsource

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util

object EventAttendeesRawToEventAttendees {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkStream")
    val sc: StreamingContext = new StreamingContext(conf, Seconds(3))

    sc.checkpoint("checkpoint")

    val kafkaParams: Map[String, String] = Map(
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.42.122:9092"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG ->"earliest"),
      (ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"),
      (ConsumerConfig.GROUP_ID_CONFIG, "EARToEA"),
    )

    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      sc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(Set("event_attendees_raw"), kafkaParams)
    )

    kafkaStream.foreachRDD(
      rdd=>{
        rdd.foreachPartition(x=>{
          val props = new util.HashMap[String, Object]()
          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"single03:9092")
          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
          val producer = new KafkaProducer[String, String](props)

          x.foreach(y=>{
            val fields: Array[String] = y.value().split(",")
            for(i<-2 to 5){
              if (fields.length>=i && fields(i-1).trim.length>0) {
                val users: Array[String] = fields(i-1).split(" ")
                for (user<-users){
                  var status="yes"
                  if (i==3)
                    status = "maybe"
                  else if(i==4)
                    status = "invited"
                  else if(i==5)
                    status = "no"
                  val info = fields(0) + "," + user + ","+status
                  println(info)
                  val record = new ProducerRecord[String, String]("event_attendees", "", info)
                  producer.send(record)
                }
              }
            }
          })
        })
      }
    )
    sc.start()
    sc.awaitTermination()

  }
}
