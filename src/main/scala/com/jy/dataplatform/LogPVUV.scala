package com.jy.dataplatform

import java.time.LocalDate

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.types.{StringType, StructField, StructType}


object LogPVUV {

  val nowdate = LocalDate.now()

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("LogPVUV")

    conf.set("hive.metastore.uris","thrift://10.10.12.198:9083")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    conf.set("hive.exec.dynamic.partition","true")
    conf.set("hive.metastore.port","9083")

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val df = spark.sql(s"select * from algorithm.dataplatform_user_action_record where hour = '${nowdate}' order by time")
//    val df = spark.sql(s"select * from algorithm.dataplatform_user_action_record where dt = '${nowdate}' order by time")

    val left = df.filter("eventid = 8.32")
    val right = df.filter("eventid = 8.33")
    val click = df.filter("eventid = 8.34")

    val totaldf = df.filter(col("eventid").equalTo("8.32") || col("eventid").equalTo("8.33") || col("eventid").equalTo("8.34"))
//    val windowSpec = Window.partitionBy("hour","uid").orderBy("time")

//    val clickdf =  df.withColumn("calc_adj_time_action",
//      when(col("eventid").equalTo("userinfo_1001") && col("eventtype").equalTo("1")
//        && when(lag("uid", 1).over(windowSpec) === col("uid"),true).otherwise(false)
//        && when(lag("eventid", 1).over(windowSpec).equalTo("product_1001"),true).otherwise(false),true).otherwise(false)
//    ).filter(col("calc_adj_time_action").equalTo(true))

    val leftPV = left.groupBy("dt").agg(count("uid") as "leftpv").orderBy("dt")
    val rightPV = right.groupBy("dt").agg(count("uid") as "rightpv").orderBy("dt")
    val clickPV = click.groupBy("dt").agg(count("uid") as "clickpv").orderBy("dt")
    val totalPV = totaldf.groupBy("dt").agg(count("uid") as "pv").orderBy("dt")

//    val leftPV = left.groupBy("hour").agg(count("uid") as "leftpv").orderBy("hour").
//    val rightPV = right.groupBy("hour").agg(count("uid") as "rightpv").orderBy("hour")
//    val clickPV = click.groupBy("hour").agg(count("uid") as "clickpv").orderBy("hour")
//    val totalPV = totaldf.groupBy("hour").agg(count("uid") as "pv").orderBy("hour")

    val UV = totaldf.groupBy("dt").agg(countDistinct("uid") as "uv").orderBy("dt")
//    val clickUV = click.groupBy("hour").agg(countDistinct("uid") as "clickuv").orderBy("hour")
//    val UV = totaldf.groupBy("hour").agg(countDistinct("uid") as "uv").orderBy("hour")

    val dataDF = leftPV.join(rightPV,Seq("dt"),"inner").join(clickPV,Seq("dt"),"inner").join(totalPV,Seq("dt"),"inner").join(UV,Seq("dt"),"inner").toDF()
//    df.groupBy("uid","time").agg("uid" -> "count").show()
    //val dataDF = leftPV.join(rightPV,Seq("hour"),"inner").join(clickPV,Seq("hour"),"inner").join(totalPV,Seq("hour"),"inner").join(UV,Seq("hour"),"inner").toDF()

//    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
//    spark.sql("set hive.exec.dynamic.partition=true")
    dataDF.coalesce(1).createOrReplaceTempView("tempTable")
    val sq = s"insert overwrite table algorithm.dataplatform_app_home_pvuv_day PARTITION(hour,dt) select leftpv,rightpv,clickpv,pv,uv,dt as hour,'${nowdate}' from tempTable"
    spark.sql(sq)
    spark.close()
  }
}

