package com.jy.dataplatform

//import com.alibaba.fastjson.{JSON, JSONException}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.LogManager
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf }
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.CanCommitOffsets
import org.apache.spark.sql.functions._

object Realtime2Hive {

//  val logger = LogManager.getLogger(Realtime2Hive.getClass.getSimpleName)
//
//  def main(args: Array[String]): Unit = {
////    val hdfspath = SparkConfig.getString("hdfs.location")
//
//    val conf = new SparkConf().setAppName("Realtime2Hive")
//
//    //启用反压
//    conf.set("spark.streaming.backpressure.enabled","true")
//    conf.set("spark.executor.extraJavaOptions", "-XX:+UseConcMarkSweepGC")
//    conf.set("hive.metastore.uris","thrift://10.1.5.13:9083")
//    conf.set("hive.exec.dynamic.partition.mode","nonstrict")
//    conf.set("spark.streaming.kafka.maxRetries", "100")
//    conf.set("spark.streaming.kafka.maxRatePerParititon", "1000")
//    conf.set("hive.exec.dynamic.partition","true")
//    conf.set("hive.metastore.port","9083")
//    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
////    val sc = new SparkContext(conf)
//
//
//    val streamContext = new StreamingContext(spark.sparkContext,Seconds(5))
//
//    //报错解决办法做checkpoint,开启checkpoint机制，把checkpoint中的数据放在这里设置的目录中，生产环境下一般放在HDFS中
////    streamContext.checkpoint("hdfs://SC01:8020/user/tmp/cp-20181201")
//
//    //direct_link equals combines kafka's topics
//    //"auto.offset.reset:earliest(每次重启重新开始消费)，latest(重启时会从最新的offset开始读取)
//    val kafkaParams = Map[String, Object](
//      "bootstrap.servers" -> "10.0.5.33:9092,10.0.5.34:9092,10.0.5.106:9092,10.2.0.68:9092,10.2.0.68:9093",
//      "key.deserializer" -> classOf[StringDeserializer],
//      "value.deserializer" -> classOf[StringDeserializer],
//      "group.id" -> "YuanfenRec",
//      "auto.offset.reset" -> "latest",
//      "enable.auto.commit" -> (false: java.lang.Boolean)
//    )
////
//    val topics = Array("commonKafka")
//    //val topics = Array("commonKafka","wireless_maker_common", "open_session")
//    val kafkaDStream = KafkaUtils.createDirectStream(
//      streamContext,
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[String, String](topics,kafkaParams)
//    )
//    //如果使用SparkStream和Kafka直连方式整合，生成的kafkaDStream必须调用foreachRDD
//    kafkaDStream.foreachRDD(kafkaRDD => {
//      if(!kafkaRDD.isEmpty()){
//        //获取当前批次的RDD的偏移量
//        val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
//        //拿出kafka中的数据
//        val lines = kafkaRDD.map(_.value())
//        //将lines字符串转换成json对象
//        val logBeanRDD = lines.map(line => {
//          var logBean: LogBean = null
//          try {
//            logBean = JSON.parseObject(line, classOf[LogBean])
//          }catch {
//            case e: JSONException => {
////              println("json解析错误！line:" + line, e)
//              logger.error("json解析错误！line:" + line, e)
//            }
//          }
//          logBean
//        })
//
//        //filter
//        val filteredRDD = logBeanRDD.filter(_ != null)
//
//        //将RDD转化成DataFrame,因为RDD中装的是case class
//        import spark.implicits._
//        val df = filteredRDD.toDF()
//          .filter($"action".equalTo("Api_Search_slide_info") || $"action".equalTo("Api_userinfo_infoquery") || $"action".equalTo("Api_Message_getMessageList"))
//          .withColumn("create_time",current_timestamp().cast("String"))
//          .cache()
//        df.show()
//
//        //将数据写到hdfs中:hdfs://hd1:9000/360
//        //df.repartition(1).write.mode(SaveMode.Append).parquet(hdfspath)
//        //df.repartition(1).write.mode(SaveMode.Append).format("hive").saveAsTable("test")
//
//        df.coalesce(1).createOrReplaceTempView("tempTable")
//
//        val sq = "insert into default.user_action select * from tempTable"
//        spark.sql(sq)
//
//        //提交当前批次的偏移量，偏移量最后写入kafka
//        kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
//      }
//    })
//
//    //启动
//    streamContext.start()
//    streamContext.awaitTermination()
////    ssc.stop(false,true)表示优雅地销毁StreamingContext对象，不能销毁SparkContext对象，
////    ssc.stop(true,true)会停掉SparkContext对象，程序就直接停了。
//    streamContext.stop(false,true)
//  }

}

//case class LogBean(action:String, uuid:String,time:String, uid:String, platform:String, client:String, chanel:String, appver:String, sex:String, ip:String, sn:String, data:String)
