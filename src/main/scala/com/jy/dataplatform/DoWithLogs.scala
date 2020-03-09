package com.jy.dataplatform

//import java.io.File
import java.net.URI
import java.text.{DateFormat, SimpleDateFormat}
import java.time.LocalDate

import org.apache.hadoop.conf.Configuration
//import java.util.Date

//import util.control.Breaks._
import com.alibaba.fastjson.{JSON, JSONException}
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.lib.input.{FileSplit, TextInputFormat}
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.sql.SparkSession
//import org.apache.spark.streaming.{Seconds, StreamingContext}

object DoWithLogs {
  val logger = LogManager.getLogger(DoWithLogs.getClass.getSimpleName)

  val nowdate = LocalDate.now()
  val start_time = nowdate + " 00:00:00"
  val end_time = nowdate + " 23:59:59"
  val df: DateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

//  private
//  hdfsconf.addResource(new Path(this.getClass.getClassLoader.getResource("hdfs-site.xml").getPath))
//  hdfsconf.addResource(new Path(this.getClass.getClassLoader.getResource("core-site.xml").getPath))

//  hdfsconf.addResource(new Path("hdfs://10.10.12.196:8020/user/algorithm/core-site.xml"))
//  hdfsconf.addResource(new Path("hdfs://10.10.12.196:8020/user/algorithm/hdfs-site.xml"))

  def FileExists(hdfs:FileSystem,spark:SparkSession): Boolean ={
    val path = "/user/algorithm/logdata"
    val p = new Path(path)
//    val hadoopConf = spark.sparkContext.hadoopConfiguration
//    val hdfs = FileSystem.get(hadoopConf)
    val allFiles = FileUtil.stat2Paths(hdfs.listStatus(p))
    allFiles.isEmpty
  }

  def SparkConfig(conf:SparkConf): SparkSession ={
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    spark
  }

  def main(args: Array[String]): Unit ={

    val conf = new SparkConf().setAppName("DoWithLogs")
    //    //启用反压
    //    conf.set("spark.streaming.backpressure.enabled","true")
    //    conf.set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC")
    conf.set("hive.metastore.uris","thrift://10.10.12.198:9083")
    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
    conf.set("hive.exec.max.dynamic.partitions","10000")
    //    conf.set("spark.streaming.kafka.maxRetries", "100")
    //    conf.set("spark.streaming.kafka.maxRatePerParititon", "1000")
    conf.set("hive.exec.dynamic.partition","true")
    conf.set("hive.metastore.port","9083")

    val hdfsconf = new Configuration()
    val hdfs:FileSystem = FileSystem.get(new URI("hdfs://nameservice1:8020"),hdfsconf)

    val spark = SparkConfig(conf)
//    var num = 1
//    val sc = spark.sparkContext
//    val streamContext = new StreamingContext(spark.sparkContext,Seconds(5))
//    breakable(
//      while(true){
    if(!FileExists(hdfs,spark)) {
//      num = 1
      val inpath = "hdfs://nameservice1:8020/user/algorithm/logdata/spm*.log"
      val fileRDD = spark.sparkContext.newAPIHadoopFile[LongWritable, Text, TextInputFormat](inpath)
      val hadoopRDD = fileRDD.asInstanceOf[NewHadoopRDD[LongWritable, Text]]
      val fileAdnLine = hadoopRDD.mapPartitionsWithInputSplit((inputSplit: InputSplit, iterator: Iterator[(LongWritable, Text)]) => {
        val file = inputSplit.asInstanceOf[FileSplit]
        iterator.map(x => {
          file.getPath.toString() + "\t" + x._2
        })
      })

      val output = fileAdnLine.map(_.split("\t", 2)).map(arr => (arr(0), arr(1)))
      val outpath = output.map(_._1)
      val data = output.map(_._2).map(_.split("\\|", 3)).map(arr => (arr(0), arr(2).trim))

      val content = data.map(x => (x._1.split(","), x._2)).map(x => ((x._1(0) + " " + x._1(1)).replace("/", "-"), x._2))

      var com = ""
      var platform = ""
      var uid = ""
      var eventid = ""
      var channel = ""
      var deviceid = ""
      var eventtype = ""
      var subeventid = ""
      var mtype = ""
      val loginfo = content.map(lines => {
        var time = lines._1

        try {
          val jsonLog = lines._2
          val data = JSON.parseObject(jsonLog)
          com = data.get("com").toString
          platform = data.get("platform").toString

          if (data.keySet().contains("uid")) {
            uid = data.get("uid").toString
          } else {
            uid = ""
          }
          if (data.keySet().contains("channel")) {
            channel = data.get("channel").toString
          } else {
            channel = ""
          }
          if (data.keySet().contains("deviceid")) {
            deviceid = data.get("deviceid").toString
          } else {
            deviceid = ""
          }
          if (data.keySet().contains("eventtype")) {
            eventtype = data.get("eventtype").toString
          } else {
            eventtype = ""
          }
          if (data.keySet().contains("eventid")) {
            eventid = data.get("eventid").toString
          } else {
            eventid = ""
          }

          if (data.keySet().contains("subeventid")) {
            subeventid = data.get("subeventid").toString
          } else {
            subeventid = ""
          }
          if (data.keySet().contains("mtype")) {
            mtype = data.get("mtype").toString
          } else {
            mtype = ""
          }
        }catch {
          case e: JSONException => {
            logger.error("json解析错误！lines:" + lines, e)
          }
        }

        val hour = time.substring(11, 13)
        val dt = time.substring(0, 10)
        LogInfo(com, time, uid, channel, deviceid, platform, eventtype, eventid, subeventid, mtype, hour, dt)
      })

      //将rdd转换成dataFrame 导入隐式转换
      import spark.implicits._
      val loginfoDF = loginfo.toDF.filter($"com".equalTo("jy") && $"uid".isNotNull && $"eventid".isNotNull && $"eventtype".isNotNull)

      loginfoDF.coalesce(1).createOrReplaceTempView("tempTable")

      //    val sq = "insert into default.log_action PARTITION(hour,dt) select com,time,uid,channel,deviceid,platform,eventtype,eventid,subeventid,mtype,substr(time,12,2) as hour,substr(time,0,10) as dt from tempTable"
      val sq = "insert into table algorithm.dataplatform_user_action_record PARTITION(hour,dt) select * from tempTable"
      spark.sql(sq)

      outpath.collect().map(new Path(_)).map(file => {
//        val hadoopConf = spark.sparkContext.hadoopConfiguration
//        val hdfs = FileSystem.get(hdfsconf)
        try {
          if (hdfs.exists(file)) {
            hdfs.delete(file, true)
          }
        }catch {
          case e: Exception => {
            logger.error("--------文件出错！file:" + file, e)
          }
        }
      })
    }
//
//    Thread.sleep(60000)  //delay 60's
//
//        }else{
//          if(num > 3){
//            break()
//          }
//          num += 1
//        }
//      }
//    )

//    println("目录底下没有文件，请查看同步文件是否产生问题！！！")
    spark.close()
  }
}


//case class LogBean(com:String, eventid:String, ip:String, time:String, eventdesc:String, mtype:String, osver:String, deviceinfo:String, deviceid2:String, brand:String, channel:String, version:String, eventtype:String, mnet:String, subeventid:String, platform:String, deviceid:String, uid:String, isp:String, extended:String, mbrand:String, mos:String, screen:String, idfa:String)
case class LogInfo(com:String, time:String, uid:String, channel:String, deviceid:String, platform:String, eventtype:String, eventid:String, subeventid:String, mtype:String, hour:String, dt:String)
