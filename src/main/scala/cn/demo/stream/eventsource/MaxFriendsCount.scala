package cn.demo.stream.eventsource

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object MaxFriendsCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("sparkStream")
    val sc = new StreamingContext(conf, Seconds(5))

    sc.checkpoint("checkPoint")

    val kafkaParams: Map[String, String] = Map(
      (ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "192.168.42.122:9092"),
      (ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer"),
      (ConsumerConfig.AUTO_OFFSET_RESET_CONFIG ->"earliest"),
      (ConsumerConfig.GROUP_ID_CONFIG, "UFRToUF2")
    )

    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      sc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(Set("user_friends_raw"), kafkaParams)
    )

    //    val value: DStream[Long] = kafkaStream.countByWindow(Seconds(15), Seconds(5))
    //    value.print()

    /*val value: DStream[(String, Int)] = kafkaStream.map(line => {
      val userFriends: Array[String] = line.value().split(",")
      if(userFriends.length==2)
      (userFriends(0), userFriends(1).split(" ").length)
      else
        (userFriends(0),0)
    }).reduceByKeyAndWindow((x:Int, y:Int) => {
      x + y
    }, Seconds(15), Seconds(5))
    value.print()*/

    val value: DStream[(String, Int)] = kafkaStream.map(line => {
      val userFriends: Array[String] = line.value().split(",")
      if (userFriends.length == 2)
        (userFriends(0), userFriends(1).split(" ").length)
      else
        (userFriends(0), 0)
    }).reduceByKeyAndWindow((x:Int, y:Int) => {
      if (x > y) x else y
    }, Seconds(15), Seconds(5))

    value.print()

    sc.start()
    sc.awaitTermination()

  }

}
