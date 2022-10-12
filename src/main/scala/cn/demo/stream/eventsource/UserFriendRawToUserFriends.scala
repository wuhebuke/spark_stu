package cn.demo.stream.eventsource

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util

/*
*
* #user_friends.csv
-------------------
user      ,                     friends
3197468391,                     1346449342 3873244116 4226080662 1222907620 547730952 1052032722 ...

转换成：
user      ,
3197468391,                     1346449342
3197468391,                     3873244116
3197468391,                     4226080662
3197468391,                     1222907620
3197468391,                     547730952
3197468391,                     1052032722
3197468391,                     ...
3197468391,                     ...
3197468391,                     ...
* */
object UserFriendRawToUserFriends {
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
      (ConsumerConfig.GROUP_ID_CONFIG, "UFRToUF1"),
    )

    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      sc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(Set("user_friends_raw"), kafkaParams)
    )

    kafkaStream.foreachRDD(
      rdd=>{
        rdd.foreachPartition(x=>{
          val props = new util.HashMap[String, Object]()
          props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"single03:9092")
          props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
          props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
          val producer = new KafkaProducer[String, String](props)

          x.foreach(y=>{   //3197468391,1346449342 3873244116 4226080662 1222907620
            val userFriends: Array[String] = y.value().toString.split(",")
            if(userFriends.length==2){
              val userId = userFriends(0)
              val friends: Array[String] = userFriends(1).split(" ")
              for (friend<-friends){
                println(userId,friend)
                val record: ProducerRecord[String, String] = new ProducerRecord[String, String]("user_friends", userId + "," + friend)
                producer.send(record)
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
