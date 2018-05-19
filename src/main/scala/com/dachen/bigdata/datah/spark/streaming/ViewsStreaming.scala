package com.dachen.bigdata.datah.spark.streaming

import java.util.Properties

import com.dachen.bigdata.datah.spark.util.{KafkaManager, KafkaUtil, RedisCluster, RedisUtil}
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object ViewsStreaming {

  private val logger = Logger.getLogger(this.getClass)
  private val pro = new Properties()
  pro.load(this.getClass.getResourceAsStream("/uat_streaming.properties"))
  private val viewCheckDir = pro.getProperty("viewCheckDir")
  private val batchDuration = pro.getProperty("batchDuration")

  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getActiveOrCreate(viewCheckDir, functionToCreateContext)
    ssc.start()
    logger.info("**********用户浏览记录数据实时流处理程序开始执行***********")
    ssc.awaitTermination()
  }

  /**
    * 创建或者从已有的checkpoint里面构建StreamingContext
    *
    * @return StreamingContext对象
    */
  def functionToCreateContext(): StreamingContext = {
    val conf = new SparkConf().setAppName("uat_viewsStreaming").set("spark.streaming.stopGracefullyOnShutdown", "true").setMaster("yarn-client")
    //初始化Streamingcontext设置为批次为一分钟
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(batchDuration.toInt))
    ssc.sparkContext.setLogLevel("WARN")
    //为ssc设置checkpoint
    ssc.checkpoint(viewCheckDir)
    //读取用户浏览记录topic的数据
    val pro = new Properties()
    pro.load(this.getClass.getResourceAsStream("/uat_consumer.properties"))
    val topicSet = pro.getProperty("viewTopics").split(",").toSet
    val brokers = pro.getProperty("brokers")
    val groupId = pro.getProperty("viewGroupId")
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "group.id" -> groupId,
      "auto.offset.reset" -> "smallest"
      //      "enable.auto.commit" -> "false",
      //       "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      //       "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )
    val km = KafkaUtil.kafka.getKafkaManager(kafkaParams)
    val topicInfo = km.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams,topicSet).persist(StorageLevel.MEMORY_AND_DISK_SER)
    topicInfo.checkpoint(Seconds(batchDuration.toInt))
    processDStream(topicInfo,km)
    ssc
  }

  def processDStream(topicInfo: DStream[(String, String)], km: KafkaManager)={

    try{
      topicInfo.foreachRDD(rdd=>{
        rdd.foreachPartition(partition=>{
          val redis = RedisCluster.cluster
          partition.foreach(line=>{
            val info = line._2.split("\001")
            redis.lpush(s"UV:${info(3)}",s"{'crid':'${info(4)}', 's': '1', 'type': '${info(2)}', 'id': '${info(1)}'}")
            redis.hset(s"UVKEY:${info(3)}",info(1),"")
          })
//          redis.close()
        })
      })
    }catch {
      case  e:Exception => e.printStackTrace()
    }
    topicInfo.foreachRDD(rdd=>km.updateZKOffsets(rdd))
  }
}

