package cn.demo.dataFrame

import cn.demo.JdbcUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.util.Properties
import scala.collection.Seq

object SparkSql_50 {
  def main(args: Array[String]): Unit = {
    val sparkApp: SparkSession = SparkSession.builder().master("local[*]").appName("sparkMysql").getOrCreate()

    val driver="com.mysql.jdbc.Driver"
    val url="jdbc:mysql://192.168.42.122:3306/school?useUnicode=true&characterEncoding=utf-8&useSSL=false"
    val user="root"
    val password="fly18"


    val prop=new Properties()
    prop.setProperty("driver",driver)
    prop.setProperty("user",user)
    prop.setProperty("password",password)


    val student: DataFrame = sparkApp.read.jdbc(url, "student", prop)
    val course: DataFrame = sparkApp.read.jdbc(url, "course", prop)
    val score: DataFrame = sparkApp.read.jdbc(url, "score", prop)
    val teacher: DataFrame = sparkApp.read.jdbc(url, "teacher", prop)

    import org.apache.spark.sql.functions._
    import sparkApp.implicits._


   //1.查询"01"课程比"02"课程成绩高的学生的信息及课程分数
    println("\"01\"课程比\"02\"课程成绩高的学生的信息及课程分数:")
    score.withColumnRenamed("s_id","stu_id").join(student,$"stu_id"===$"s_id" and($"c_id")===1).alias("ods1").
      join(
      score.withColumnRenamed("s_id","stu_id").join(student,$"stu_id"===$"s_id" and($"c_id")===2).alias("ods2"),
      col("ods1.stu_id")===col("ods2.stu_id")
    ).where($"ods1.s_score">$"ods2.s_score").select(
      $"ods1.s_id",$"ods1.s_name",$"ods1.s_birth",$"ods1.s_sex",$"ods1.s_score".as("score_01"),$"ods2.s_score".as("score_02"))
     .show()

    //2、查询"01"课程比"02"课程成绩低的学生的信息及课程分数
    /*println("\"01\"课程比\"02\"课程成绩低的学生的信息及课程分数")
    score.withColumnRenamed("s_id","stu_id").join(student,$"stu_id"===$"s_id" and($"c_id")===1).alias("ods1").
      join(
        score.withColumnRenamed("s_id","stu_id").join(student,$"stu_id"===$"s_id" and($"c_id")===2).alias("ods2"),
        col("ods1.stu_id")===col("ods2.stu_id")
      ).where($"ods1.s_score"<$"ods2.s_score").select(
      $"ods1.s_id",$"ods1.s_name",$"ods1.s_birth",$"ods1.s_sex",$"ods1.s_score".as("score_01"),$"ods2.s_score".as("score_02"))
     .show()*/


    //3.查询平均成绩大于等于60分的同学的学生编号和学生姓名和平均成绩
    /*println("平均成绩大于等于60分的同学的学生编号和学生姓名和平均成绩")
    score.groupBy("s_id").agg(avg("s_score").as("avg")).alias("ods").join(student,Seq("s_id")).where($"avg">=60)
      .select("s_id","s_name","avg")
      .show()*/

    //4.查询平均成绩小于60分的同学的学生编号和学生姓名和平均成绩 (包括有成绩的和无成绩的)
    /*println("平均成绩小于60分的同学的学生编号和学生姓名和平均成绩")
    student.join(score.groupBy("s_id").agg(avg("s_score").as("avg")).alias("ods"),Seq("s_id"),"left_outer")
      .where($"avg"<60||$"avg".isNull)
      .select("s_id","s_name","avg")
      .show()*/

    //5、查询所有同学的学生编号、学生姓名、选课总数、所有课程的总成绩
   /* println("查询所有同学的学生编号、学生姓名、选课总数、所有课程的总成绩")
    student.join(
      score.groupBy("s_id").agg(sum("s_score").as("totalScore"),count("c_id")
        .as("sumCourse")),Seq("s_id"),"left_outer")
      .select($"s_id",$"s_name",$"sumCourse",$"totalScore")
      .sort(asc("s_id"))
      .show()*/

    //6、查询"李"姓老师的数量
    //    println("查询\"李\"姓老师的数量")
    //    val cnt: Long = teacher.where("t_name like '李%'").count()
    //    println(cnt)

    //7.查询学过"张三"老师授课的同学的信息
     /*println("查询学过\"张三\"老师授课的同学的信息")
    teacher.join(course,Seq("t_id"))
      .join(score,Seq("c_id")).where($"t_name"==="张三")
      .join(student,Seq("s_id"))
      .select("s_id","s_name","s_birth","s_sex").sort(asc("s_id"))
      .show()*/

    //8、查询没学过"张三"老师授课的同学的信息
    /*println("查询没学过\"张三\"老师授课的同学的信息")
    student.join(
      student
        .join(score,Seq("s_id"),"left_outer")
        .join(course,Seq("c_id"),"left_outer")
        .join(teacher,Seq("t_id"),"left_outer")
        .where($"t_name"==="张三")
        .select("s_id").alias("ods")
      ,Seq("s_id"),"left_outer")
      .where($"ods.s_id".isNull )
      .show()*/

    //9、查询学过编号为"01"并且也学过编号为"02"的课程的同学的信息
   /* println("查询学过编号为\"01\"并且也学过编号为\"02\"的课程的同学的信息")
    student
      .join(score,Seq("s_id")).join(course,Seq("c_id")).where($"c_id"===1).select("s_id")
      .join(student.join(score,Seq("s_id")).join(course,Seq("c_id")).where($"c_id"===2),Seq("s_id"))
      .sort("s_id").select("s_id","s_name","s_birth","s_sex")
      .show()*/

    //10.查询学过编号为"01"但是没有学过编号为"02"的课程的同学的信息
    /*println("查询学过编号为\"01\"但是没有学过编号为\"02\"的课程的同学的信息")
    student.join(score,Seq("s_id")).where($"c_id"===1).select("s_id")
      .join(
        student.join(score,Seq("s_id")).where($"c_id"===2).select("s_id").alias("ods2"),Seq("s_id"),"left_outer")
      .where($"ods2.s_id".isNull)
      .join(student,Seq("s_id"))
      .show()*/

    //11、查询没有学全所有课程的同学的信息
    /*println("查询没有学全所有课程的同学的信息 ")
    student.join(
      student.join(
              student
                .join(score,Seq("s_id"),"left_outer")
                .join(course,Seq("c_id"),"left_outer")
                .join(teacher,Seq("t_id"),"left_outer")
                .select("s_id").alias("ods")
              ,Seq("s_id"),"left_outer"
                   )
        .groupBy("s_id")
        .agg(count("s_id").alias("count")).alias("ods"),Seq("s_id")
        ).where($"ods.count"<3).sort(asc("s_id"))
      .show()*/

    //12、查询至少有一门课与学号为"01"的同学所学相同的同学的信息
    /*println("查询至少有一门课与学号为\"01\"的同学所学相同的同学的信息")
    student.where($"s_id"=!=1)
     .join(
       student
         .join(score,Seq("s_id"),"left_outer")
         .join(course,Seq("c_id"),"left_outer")
         .where($"c_id".isNull).select("s_id").alias("ods"),Seq("s_id"),"left_outer")
     .where($"ods.s_id".isNull)
     .sort(asc("s_id"))
     .show()*/

    //13、查询和"01"号的同学学习的课程完全相同的其他同学的信息
    /*println("查询和\"01\"号的同学学习的课程完全相同的其他同学的信息")
    score.filter('s_id===1)
      .select(count("c_id").as("cid_count"),collect_set("c_id").as("01_All_cid"))
      .as("ods2")
      .join(
        score.filter($"s_id"=!=1).groupBy("s_id").agg(count("c_id").as("cid_other"),collect_set("c_id")
          .as("other_cid"))
          .as("ods1"))
      .where(($"cid_other"===$"cid_count") and col("ods1.other_cid")===col("ods2.01_All_cid")  ).select("s_id")
      .join(student,Seq("s_id"))
      .show()*/

/*    score.filter('s_id === 1)
      .select(count("c_id").alias("cid_count"),collect_set("c_id").alias("01_All_cid"))
      .join(score.filter('s_id =!= 1)).alias("score1")
                .groupBy("s_id","01_All_cid","cid_count")
                .agg(
                  count("score1.c_id").alias("cnt"),
                  count(array_contains(col("01_All_cid"),col("score1.c_id")) === "true").alias("sameCnt"))
                .where(col("cid_count") === col("cnt") and col("cnt") === col("sameCnt"))
                .select("s_id").alias("s_ids")
                .join(student, col("s_ids.s_id")===student("s_id"))
                .select(student("s_id"),student("s_name"),student("s_birth"),student("s_sex")).sort(asc("s_id"))
      .show()*/

    //14、查询没学过"张三"老师讲授的任一门课程的学生姓名
    /*println("查询没学过\"张三\"老师讲授的任一门课程的学生姓名")
    student
      .join( teacher.filter('t_name==="张三").select("t_id")
      .join(course,Seq("t_id")).select("c_id")
      .join(score,Seq("c_id")).select("s_id").as("ods"),Seq("s_id"),"left_outer")
      .where($"ods.s_id".isNull)
      .show()*/

    //15、查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩
    /*println("查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩")
    student.join(
    score.filter('s_score<60).groupBy("s_id")
      .agg(count("s_id").as("underCount"),round(avg("s_score"),2).as("avg"))
      .where($"underCount">1)
      .select("s_id","avg").as("ods")
      ,Seq("s_id"))
      .show()*/

    //16、检索"01"课程分数小于60，按分数降序排列的学生信息
    /*println("检索\"01\"课程分数小于60，按分数降序排列的学生信息")
    score.filter('c_id===1 and($"s_score"<60))
      .join(student,Seq("s_id"))
      .sort(desc("s_score"))
      .show()*/

    //17、按平均成绩从高到低显示所有学生的所有课程的成绩以及平均成绩
    /*println("按平均成绩从高到低显示所有学生的所有课程的成绩以及平均成绩")
      score.join(course,Seq("c_id"))
        .where($"c_name"==="语文")
        .withColumnRenamed("s_score","语文")
        .select("s_id","语文").as("cn")
      .join(score.join(course,Seq("c_id"))
        .where($"c_name"==="数学")
        .withColumnRenamed("s_score","数学")
        .select("s_id","数学").as("math"),Seq("s_id"),"outer")
      .join(score.join(course,Seq("c_id"))
        .where($"c_name"==="英语")
        .withColumnRenamed("s_score","英语")
        .select("s_id","英语").as("en"),Seq("s_id"),"outer")
      .join( score.groupBy("s_id")
        .agg(round(avg("s_score"),2).as("avg")),Seq("s_id"),"outer")
      .sort(desc("avg"))
      .show()*/

    //18.查询各科成绩最高分、最低分和平均分：以如下形式显示：课程ID，课程name，最高分，最低分，平均分，及格率，中等率，优良率，优秀率
    //   及格为>=60，中等为：70-80，优良为：80-90，优秀为：>=90
    //println("查询各科:课程ID，课程name，最高分，最低分，平均分，及格率，中等率，优良率，优秀率")
   /* score.groupBy("c_id")
      .agg(
        max("s_score").as("max"),
        min("s_score").as("min"),
        round(avg("s_score"),2).as("avg"))
      .join(
        score.filter($"s_score">=60).groupBy("c_id").count().withColumnRenamed("count","passCount").as("ods1")
          .join(score.groupBy("c_id").count().as("ods2"),Seq("c_id"))
          .select($"c_id",round($"passCount"/$"count",2).as("pass_per")),Seq("c_id"),"outer"
      )
      .join(
        score.filter($"s_score">=70 && $"s_score"<80).groupBy("c_id").count().withColumnRenamed("count","midCount")
          .join(score.groupBy("c_id").count(),Seq("c_id"))
          .select($"c_id",round($"midCount"/$"count",2).as("mid_per")),Seq("c_id"),"outer"
      )
      .join(
        score.filter($"s_score">=80 && $"s_score"<90).groupBy("c_id").count().withColumnRenamed("count","goodCount")
          .join(score.groupBy("c_id").count(),Seq("c_id"))
          .select($"c_id",round($"goodCount"/$"count",2).as("good_per")),Seq("c_id"),"outer"
      )
      .join(score.filter($"s_score">=90).groupBy("c_id").count().withColumnRenamed("count","exceCount")
        .join(score.groupBy("c_id").count(),Seq("c_id"))
        .select($"c_id",round($"exceCount"/$"count",2).as("exce_per")),Seq("c_id"),"outer"
      )
      .join(course,Seq("c_id")).select(
      "c_id", "c_name", "max", "min",
      "avg", "pass_per", "mid_per", "good_per", "exce_per")
      .show()*/

    //19、按各科成绩进行排序，并显示排名
    //println("按各科成绩进行排序，并显示排名")
    /*score
      .join(student,"s_id")
      .join(course,"c_id")
      .selectExpr("s_id","s_name","c_id","c_name","s_score",
        "row_number() over(distribute by c_id sort by s_score desc) rank")
      .sort("c_id","rank")
      .show()*/

    //20.查询学生的总成绩并进行排名
    //println("查询学生的总成绩并进行排名")
   /* student.join(
      score,"s_id")
      .selectExpr("s_id","s_name","sum(s_score) over(distribute by s_id) as sum_score").distinct()
      .selectExpr("s_id","s_name","sum_score","row_number() over(sort by sum_score desc) rank")
      .show()*/

    //21、查询不同老师所教不同课程平均分从高到低显示
    //println("21.查询不同老师所教不同课程平均分从高到低显示 ")

  }
}
