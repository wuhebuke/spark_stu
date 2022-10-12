package cn.demo.eventSource

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object DataAnalysis {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("eventsource").setMaster("local[*]")
    val sparkApp: SparkSession = SparkSession.builder().config(conf)
      .config("hive.metastore.uris","thrift://single03:9083")
      .enableHiveSupport().getOrCreate()

    import sparkApp.implicits._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.types._

   /* val userSRDD: RDD[String] = sc.textFile("hdfs://192.168.42.122:9000/eventsource/users.csv")
//    userSRDD.take(10).foreach(println)
    userSRDD.collect().map(x=>x.split(",")).take(10).foreach(x=>println(x.toList))*/


    val dfUsers = sparkApp.read.format("csv").option("header","true").load("hdfs://single03:9000/eventsource/users.csv")

    //=================================================数据探索DWD========================================================
    // 查看birthyear列中的数据情况
    val dfUsersBirth: DataFrame = dfUsers.select(col("user_id"), col("birthyear").cast(IntegerType).as("f_birthyear"), col("birthyear"))
    //  dfUsersBirth.printSchema()

    //找出users.csv 中birthyear为空的列数
    val birthYearNull_count: Long = dfUsersBirth.filter(col("f_birthyear").isNull).count() //1494

    //找出平均年龄
    val dfAvgAge: DataFrame = dfUsersBirth.select(avg(col("f_birthyear")).cast(IntegerType).as("avg_year"))

    val dfUser2: DataFrame = dfUsersBirth.crossJoin(dfAvgAge)
      .withColumn("new_birthyear", when(col("f_birthyear").isNull, col("avg_year")).otherwise(col("f_birthyear")))
      .select(col("user_id"), col("new_birthyear"), col("birthyear"))

    val count2: Long = dfUser2.filter($"new_birthyear".isNull).count()


    //性别探索
    //    dfUsers.select(col("gender")).distinct().show()
    //    dfUsers.select(col("gender"),col("user_id")).groupBy(col("gender")).count().show()

    //新增一列，将性别为空的字段填充为unknown
    val dfUserGender = dfUsers.withColumn("gender1", when(col("gender").isNull, lit("unknow")).otherwise(col("gender")))

    //    dfUserGender.select(col("gender1"),col("user_id")).groupBy(col("gender1")).count()

    val dfUserGender2: DataFrame = dfUserGender.drop(col("gender")).withColumnRenamed("gender1", "gender")

    //------------------------------------------------------------------------------------------------------------------

    val dfEvent: DataFrame = sparkApp.read.format("csv").option("header", "true").load("hdfs://single03:9000/eventsource/events.csv")

    //是否有重复event_id
    dfEvent.select($"event_id").distinct().count() //3137972 无重复

    //是否存在事件没有主办者
    dfEvent.filter($"user_id".isNull).count()

    //user创建event个数的排行
//    dfEvent.groupBy($"user_id").count().sort(desc("count")).show()

    dfEvent.createOrReplaceTempView("events")
//    sparkApp.sql("""select user_id,count(*) cnt from events group by user_id order by cnt desc limit 3""").show()

    dfEvent.as("eve")
      .join(
        dfUsers.withColumnRenamed("user_id","user_id2").as("use"),
        $"eve.user_id"===$"use.user_id2","right")
      .filter($"user_id2".isNull)

   /* dfUsers.createOrReplaceTempView("users")
    sparkApp.sql(
      """
        |select count(*)
        |from events e inner join users u
        |on e.user_id=u.user_id
        |""".stripMargin).show(20)

    sparkApp.sql(
      """
        select count(*)
        from events where start_time regexp '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*'
        """).show(20)*/

    //------------------------------------------------------------------------------------------------------------------

    val dfUserFriends = sparkApp.read.format("csv").option("header", "true").load("hdfs://single03:9000/eventsource/user_friends.csv")

    //是否有相同的user
    //val num = dfUserFriends.select($"user").distinct().count()  //38202

    val dfUF: DataFrame = dfUserFriends.withColumn("friend_id", explode(split($"friends", " "))).drop("friends").withColumnRenamed("user", "user_id")

    //有16条重复数据
    val dfUFCount: Dataset[Row] = dfUF.groupBy($"user_id", $"friend_id").agg(count($"user_id").as("cnt")).filter($"cnt" > 1)

    //------------------------------------------------------------------------------------------------------------------
    val dfEventAttendees: DataFrame = sparkApp.read.format("csv").option("header", "true").load("hdfs://single03:9000/eventsource/event_attendees.csv")

    val dfYes: DataFrame = dfEventAttendees.select($"event", $"yes")
      .withColumn("user_id", explode(split($"yes", " ")))
      .withColumnRenamed("event", "event_id")
      .drop($"yes").withColumn("attend_type", lit("yes"))


    val dfNo = dfEventAttendees.select($"event", $"no")
      .withColumn("user_id", explode(split($"no", " ")))
      .withColumnRenamed("event", "event_id")
      .drop($"no").withColumn("attend_type", lit("no"))

    val dfMaybe = dfEventAttendees.select($"event", $"maybe")
      .withColumn("user_id", explode(split($"maybe", " ")))
      .withColumnRenamed("event", "event_id")
      .drop($"maybe").withColumn("attend_type", lit("maybe"))

    val dfInvited = dfEventAttendees.select($"event", $"invited")
      .withColumn("user_id", explode(split($"invited", " ")))
      .withColumnRenamed("event", "event_id")
      .drop($"invited").withColumn("attend_type", lit("invited"))

    val dfRst = dfYes.union(dfNo).union(dfMaybe).union(dfInvited)
//    dfRst.show(10,false)

    val dfRst2 = Seq("yes", "no", "maybe", "invited").map(
      x => dfEventAttendees.select($"event".as("event_id"), col(x))
        .withColumn("user_id", explode(split(col(x), " ")))
        .withColumn("attend_type", lit(x)).drop(col(x))
    ).reduce((x, y) => x.union(y))

//    println(dfRst2.count())


    //=================================================数据探索DWS========================================================
    //每一个用户的朋友数
    val user_friend_count: DataFrame = dfUF.groupBy($"user_id").agg(count($"friend_id").as("friend_id"))
      .filter($"friend_id".isNotNull and trim($"friend_id") =!= "")

    //对于 event_attendee 每一件事件不同状态对应的人数
    val event_attendee_count: DataFrame = dfRst.groupBy($"event_id", $"attend_type").agg(count($"user_id").as("attendCount"))

    //事件中用户的四种状态
    val event_user_stateDF: DataFrame = dfRst.withColumnRenamed("user_id", "friend_id")
      .withColumn("invited", when($"attend_type" === "invited", 1).otherwise(0))
      .withColumn("no", when($"attend_type" === "no", 1).otherwise(0))
      .withColumn("yes", when($"attend_type" === "yes", 1).otherwise(0))
      .withColumn("maybe", when($"attend_type" === "maybe", 1).otherwise(0))
      .drop($"attend_type")

    //attend_user_id指被邀请的用户id，此表用来描述被邀请用户对某一事件的各种状态
    val user_event_status: DataFrame = event_user_stateDF.withColumnRenamed("friend_id", "attend_user_id")
      .groupBy($"event_id", $"attend_user_id").agg(
      max($"invited").as("invited"),
      max($"no").as("no_attended"),
      max($"yes").as("attended"),
      max($"maybe").as("maybe_attended"))

    //用户(被邀请)参加事件的数量
    val user_attended_event_count: DataFrame = user_event_status.groupBy($"attend_user_id")
      .agg(
        sum($"invited").as("invited_count"),
        sum($"attended").as("attended_count"),
        sum($"no_attended").as("no_count"),
        sum($"maybe_attended").as("maybe_count"))

    //用户的朋友在所有事件参与的状态
    val friend_attendevent_state: DataFrame = dfUF.join(user_event_status,$"friend_id"===$"attend_user_id","left")
      .select($"user_id",$"friend_id",$"event_id",
        when($"invited">0,1).otherwise(0).as("invited"),
        when($"attended">0,1).otherwise(0).as("attended"),
        when($"no_attended">0,1).otherwise(0).as("no_attended"),
        when($"maybe_attended">0,1).otherwise(0).as("maybe_attended"),
      )

    //用户 朋友中对某一事件有多少个被邀请，多少个去，多少个不去，多少个可能去
    val friend_attend_summary: Dataset[Row] = friend_attendevent_state.groupBy($"user_id", $"event_id").agg(
      sum($"invited").as("invited_friends_count"),
      sum($"attended").as("attended_friends_count"),
      sum($"no_attended").as("no_attended_friends_count"),
      sum($"maybe_attended").as("maybe_attended_friends_count")
    ).filter($"event_id".isNotNull)

    //------------------------------------------------------------------------------------------------------------------

    //举办事件数量城市排行
    dfEvent.groupBy($"city").agg(count($"user_id").as("cnt"))
      .select(
        when($"city".isNotNull,$"city").otherwise("unknown").as("city"), $"cnt").sort(desc("cnt"))
      .withColumn("level",row_number() over Window.partitionBy().orderBy(desc("cnt")))
      .show()

   /* sparkApp.sql(
      """
        |select
        |       t.city,
        |       t.cnt,
        |       row_number() over (order by t.cnt desc ) level
        |from(
        |select
        |       `if`(city!="",city,"unknown") city,
        |       count(*) cnt from dwd_events.events
        |group by city
        |order by cnt desc limit 32) t
        |""".stripMargin).show()*/

  }
}
