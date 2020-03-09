package com.jy.dataplatform

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class LogBean(action:String, uuid:String,time:String, uid:String, platform:String, client:String, chanel:String, appver:String, sex:String, ip:String, sn:String, data:String, dt:String)

object PVUV {

//  def main(args: Array[String]): Unit = {
//
//    val conf = new SparkConf().setAppName("Realtime2Hive")
//
//    conf.set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC")
//    conf.set("hive.metastore.uris","thrift://10.1.5.13:9083")
//    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
//    conf.set("hive.exec.dynamic.partition","true")
//    conf.set("hive.metastore.port","9083")
//    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
////    val sc = new SparkContext(conf)
//
////    val textFile = spark.sparkContext.textFile("hdfs://localhost:9000/user/root/input/1000_log")
////    val textRDD = textFile.map(_.split("\t"))
//    import spark.implicits._
//    val elecDF = spark.sparkContext
//      .textFile("hdfs://localhost:9000/user/root/input/1000_log")
//      .map(_.split("\t"))
//      .map(attr => LogBean(attr(0),attr(1),attr(2),attr(3), attr(4),attr(5), attr(6),attr(7), attr(8),attr(9), attr(10),attr(11)))
//      .toDF().where($"dt".substr(0, 10) === date_sub(current_date(),-1).cast("String"))
//
//    //统计pv
//    val pv = elecDF.filter($"action".equalTo("Api_userinfo_infoquery") || $"action".equalTo("Api_Search_slide_info")).count()
//
//    //统计uv
//    val uv = elecDF.filter($"action".equalTo("Api_userinfo_infoquery") || $"action".equalTo("Api_Search_slide_info")).select("uid").distinct().count()
//
////    //统计uv
////    val uv = elecDF.filter($"action".equalTo("Api_userinfo_infoquery") || $"action".equalTo("Api_Search_slide_info")).groupBy().count()
//
//    elecDF.createOrReplaceTempView("elec")
//    val distinctDF = spark.sql("select count(0) ")
//
//  }

}
