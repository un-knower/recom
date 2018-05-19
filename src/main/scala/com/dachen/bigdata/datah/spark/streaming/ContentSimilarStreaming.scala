package com.dachen.bigdata.datah.spark.streaming

import java.nio.file.FileSystems
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import breeze.linalg.{SparseVector, norm}
import com.dachen.bigdata.datah.spark.util.{KafkaManager, KafkaUtil, RedisCluster, RedisUtil}
import com.huaban.analysis.jieba.{JiebaSegmenter, WordDictionary}
import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.feature.{HashingTF, IDF, IDFModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.mllib.linalg.{Vector, SparseVector => SV}
object ContentSimilarStreaming {



  @volatile private var bTFVectorRDD: Broadcast[RDD[(String, Vector)]] = null
  @volatile private var bIDFVectorArray: Broadcast[Array[(String, Vector)]] = null
  @volatile private var bIDFModel: Broadcast[IDFModel] = null
  @volatile private var bTagArray: Broadcast[Array[String]] = null
  private val logger = Logger.getLogger(this.getClass)
  private val pro = new Properties()
  pro.load(this.getClass.getResourceAsStream("/uat_streaming.properties"))
  private val contentSimilarCheckDir = pro.getProperty("contentSimilarCheckDir")
  private val batchDuration = pro.getProperty("batchDuration")
  private val dictPath = pro.getProperty("dictPath")
  private val labelPath = pro.getProperty("labelPath")
  private val contentPath = pro.getProperty("contentPath")


  def getTFVectorRDD(sc: SparkContext): Broadcast[RDD[(String, Vector)]] = {
    if (bTFVectorRDD == null) {
      synchronized {
        if (bTFVectorRDD == null) {
          val yesterday = getYesterday()
          val path = s"${contentPath}/dt=${yesterday}"
          val textRDD = sc.textFile(path)
          bTFVectorRDD = sc.broadcast(getContentVector(textRDD))
        }
      }
    }
    bTFVectorRDD
  }

  def getIDFModel(sc: SparkContext, tfRDD:RDD[(String, Vector)]): Broadcast[IDFModel] = {
    if (bIDFModel == null) {
      synchronized {
        if (bIDFModel == null) {
          //          val tfRDD = getTFVectorRDD(sc).value
          val idfModel = new IDF().fit(tfRDD.values)
          bIDFModel = sc.broadcast(idfModel)
        }
      }
    }
    bIDFModel
  }
  def getIDFVectorArray(sc: SparkContext, tfRDD:RDD[(String, Vector)]): Broadcast[Array[(String, Vector)]] = {
    if (bIDFVectorArray == null) {
      synchronized {
        if (bIDFVectorArray == null) {
          //          val tfRDD = getTFVectorRDD(sc).value
          val idfModel = getIDFModel(sc, tfRDD).value
          val idfRDD = tfRDD.mapValues(v => idfModel.transform(v))
          bIDFVectorArray = sc.broadcast(idfRDD.collect())
        }
      }
    }
    bIDFVectorArray
  }

  def getTagArray(sc: SparkContext): Broadcast[Array[String]] = {
    if (bTagArray == null) {
      synchronized {
        if (bTagArray == null) {
          val tagArray = sc.textFile(labelPath).collect()
          bTagArray = sc.broadcast(tagArray)
        }
      }
    }
    bTagArray
  }
  def unpersistTFVectorRDD(sc: SparkContext) = {
    if(bTFVectorRDD != null){
      bTFVectorRDD.unpersist()
    }
  }

  def unpersistIDFModel(sc: SparkContext) = {
    if(bIDFModel != null){
      bIDFModel.unpersist()
    }
  }
  def unpersistIDFVectorArray(sc: SparkContext) = {
    if(bIDFVectorArray != null){
      bIDFVectorArray.unpersist()
    }
  }

  def main(args: Array[String]): Unit = {


    val ssc = StreamingContext.getActiveOrCreate(contentSimilarCheckDir, functionToCreateContext)
    ssc.start()
    logger.info("**********新增内容数据计算相似度实时流处理程序开始执行***********")
    ssc.awaitTermination()
  }

  /**
    * 创建或者从已有的checkpoint里面构建StreamingContext
    *
    * @return StreamingContext对象
    */
  def functionToCreateContext(): StreamingContext = {

    val conf = new SparkConf().setAppName("uat_contentSimilarStreaming").set("spark.streaming.stopGracefullyOnShutdown", "true").setMaster("yarn-client")
    //初始化Streamingcontext设置为批次为一分钟
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(batchDuration.toInt))
    ssc.sparkContext.setLogLevel("WARN")
    //为ssc设置checkpoint
    ssc.checkpoint(contentSimilarCheckDir)
    //读取新增内容topic的数据
    val pro = new Properties()
    pro.load(this.getClass.getResourceAsStream("/uat_consumer.properties"))
    val topicSet = pro.getProperty("contentSimilarTopic").split(",").toSet
    val brokers = pro.getProperty("brokers")
    val groupId = pro.getProperty("contentSimilarGroupId")
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
    val toPath = FileSystems.getDefault.getPath(dictPath)

    //使用结巴分词，加载自己的词库
    WordDictionary.getInstance().loadUserDict(toPath)

    processDStream(topicInfo,km)
    ssc
  }

  def processDStream(topicInfo: DStream[(String, String)], manager: KafkaManager)={

    //    var flag:Boolean = true
    //    var contentVector:RDD[(String,Vector)] = null
    var date:String = getTomorrow()
    val tagArray = getTagArray(topicInfo.context.sparkContext).value
    topicInfo.repartition(1).map(_._2).foreachRDD(rdd=>{

      if(convertTimeStamp2DateStr(System.currentTimeMillis(),"yyyy-MM-dd").equals(date)){

        val config = new Configuration()
        val fileSystem = FileSystem.get(config)
        val yesterday = getYesterday()
        val path = s"${contentPath}/dt=${yesterday}"
        if(fileSystem.exists(new Path(path))){
          //          tagArray = rdd.sparkContext.textFile(labelPath).collect()
          unpersistTFVectorRDD(rdd.sparkContext)
          unpersistIDFVectorArray(rdd.sparkContext)
          unpersistIDFModel(rdd.sparkContext)

          date = getTomorrow()
          logger.warn(s"****************${convertTimeStamp2DateStr(System.currentTimeMillis(),"yyyyMMdd HH:mm:ss")}  读取成功*********************")
          //          flag = false
        }
      }




      var tf:Vector = null
      var id:String = null
      var itemType:String = null
      var circleID:String = null
      var editor:String = null
      var dept:String = null
      var label:String = null
      var content:String = null
      var skill:String = null
      var contentTag:List[String] = null
      //      if(!rdd.isEmpty){
      val tfRDD = rdd.mapPartitions(partition=> {
        val jieBa = new JiebaSegmenter()
        partition.map(line => {
          val info = line.split("\001")
          info(0) match {
            case "publishFaq" =>
              if (info.length != 14) {
                logger.warn("publishFaq格式不匹配")
              } else {
                itemType = "1"
                id = info(1)
                circleID = info(3)
                label = info(5)
                content = info(7)
                editor = info(11)
                dept = info(12)
                skill = info(13)
                contentTag = List()
                if (!"".equals(label) && !"\\N".equals(label) && !"null".equalsIgnoreCase(label)) {
                  contentTag = label :: contentTag
                }
                if (!"".equals(editor) && !"\\N".equals(editor) && !"null".equalsIgnoreCase(editor)) {
                  contentTag = editor :: contentTag
                }
                if (!"".equals(dept) && !"\\N".equals(dept) && !"null".equalsIgnoreCase(dept)) {
                  contentTag = dept :: contentTag
                }
                val contentArray = jieBa.sentenceProcess(content)
                for (i <- 0 until contentArray.size()) {
                  if (tagArray.contains(contentArray.get(i))) {
                    contentTag = contentArray.get(i) :: contentTag
                  }
                }
                val skillArray= jieBa.sentenceProcess(skill)
                for(i <- 0 until skillArray.size()){
                  if(tagArray.contains(skillArray.get(i))){
                    contentTag = skillArray.get(i)::contentTag
                  }
                }
                if (!contentTag.isEmpty) {
                  tf = new HashingTF(Math.pow(2, 18).toInt).transform(contentTag)
                } else tf = null
              }

            case "publishQuestion" =>
              if (info.length != 16) {
                logger.warn("publishQuestion格式不匹配")
              } else {
                itemType = "2"
                id = info(1)
                circleID = info(3)
                label = info(5)
                content = info(7)
                editor = info(13)
                dept = info(14)
                skill = info(15)
                contentTag = List()
                if (!"".equals(label) && !"\\N".equals(label) && !"null".equalsIgnoreCase(label)) {
                  contentTag = label :: contentTag
                }
                if (!"".equals(editor) && !"\\N".equals(editor) && !"null".equalsIgnoreCase(editor)) {
                  contentTag = editor :: contentTag
                }
                if (!"".equals(dept) && !"\\N".equals(dept) && !"null".equalsIgnoreCase(dept)) {
                  contentTag = dept :: contentTag
                }
                val contentArray = jieBa.sentenceProcess(content)
                for (i <- 0 until contentArray.size()) {
                  if (tagArray.contains(contentArray.get(i))) {
                    contentTag = contentArray.get(i) :: contentTag
                  }
                }
                val skillArray= jieBa.sentenceProcess(skill)
                for(i <- 0 until skillArray.size()){
                  if(tagArray.contains(skillArray.get(i))){
                    contentTag = skillArray.get(i)::contentTag
                  }
                }
                if (!contentTag.isEmpty) {
                  tf = new HashingTF(Math.pow(2, 18).toInt).transform(contentTag)
                } else tf = null
              }

            case "publishReward" =>
              if (info.length != 14) {
                logger.warn("publishReward格式不匹配")
              } else {
                itemType = "3"
                id = info(1)
                circleID = info(3)
                label = info(5)
                content = info(7)
                editor = info(11)
                dept = info(12)
                skill = info(13)

                contentTag = List()
                if (!"".equals(label) && !"\\N".equals(label) && !"null".equalsIgnoreCase(label)) {
                  contentTag = label :: contentTag
                }
                if (!"".equals(editor) && !"\\N".equals(editor) && !"null".equalsIgnoreCase(editor)) {
                  contentTag = editor :: contentTag
                }
                if (!"".equals(dept) && !"\\N".equals(dept) && !"null".equalsIgnoreCase(dept)) {
                  contentTag = dept :: contentTag
                }
                val contentArray = jieBa.sentenceProcess(content)
                for (i <- 0 until contentArray.size()) {
                  if (tagArray.contains(contentArray.get(i))) {
                    contentTag = contentArray.get(i) :: contentTag
                  }
                }
                val skillArray= jieBa.sentenceProcess(skill)
                for(i <- 0 until skillArray.size()){
                  if(tagArray.contains(skillArray.get(i))){
                    contentTag = skillArray.get(i)::contentTag
                  }
                }
                if (!contentTag.isEmpty) {
                  tf = new HashingTF(Math.pow(2, 18).toInt).transform(contentTag)
                } else tf = null
              }

            case "publishDisease" =>
              if (info.length != 12) {
                logger.warn("publishDisease格式不匹配")
              } else {
                itemType = "4"
                id = info(1)
                circleID = info(3)
                label = info(4)
                content = info(6)
                editor = info(9)
                dept = info(10)
                skill = info(11)
                contentTag = List()
                if (!"".equals(label) && !"\\N".equals(label) && !"null".equalsIgnoreCase(label)) {
                  contentTag = label :: contentTag
                }
                if (!"".equals(editor) && !"\\N".equals(editor) && !"null".equalsIgnoreCase(editor)) {
                  contentTag = editor :: contentTag
                }
                if (!"".equals(dept) && !"\\N".equals(dept) && !"null".equalsIgnoreCase(dept)) {
                  contentTag = dept :: contentTag
                }
                val contentArray = jieBa.sentenceProcess(content)
                for (i <- 0 until contentArray.size()) {
                  if (tagArray.contains(contentArray.get(i))) {
                    contentTag = contentArray.get(i) :: contentTag
                  }
                }
                val skillArray= jieBa.sentenceProcess(skill)
                for(i <- 0 until skillArray.size()){
                  if(tagArray.contains(skillArray.get(i))){
                    contentTag = skillArray.get(i)::contentTag
                  }
                }
                if (!contentTag.isEmpty) {
                  tf = new HashingTF(Math.pow(2, 18).toInt).transform(contentTag)
                } else tf = null
              }

            case "publishBroadcast" =>
              if (info.length != 15) {
                logger.warn("publishBroadcast格式不匹配")
              } else {
                if ("live".equals(info(6))) {
                  itemType = "5"
                  id = info(1)
                  circleID = info(3)
                  label = info(5)
                  content = info(9)
                  editor = info(12)
                  dept = info(13)
                  skill = info(14)
                  contentTag = List()
                  if (!"".equals(label) && !"\\N".equals(label) && !"null".equalsIgnoreCase(label)) {
                    contentTag = label :: contentTag
                  }
                  if (!"".equals(editor) && !"\\N".equals(editor) && !"null".equalsIgnoreCase(editor)) {
                    contentTag = editor :: contentTag
                  }
                  if (!"".equals(dept) && !"\\N".equals(dept) && !"null".equalsIgnoreCase(dept)) {
                    contentTag = dept :: contentTag
                  }
                  val contentArray = jieBa.sentenceProcess(content)
                  for (i <- 0 until contentArray.size()) {
                    if (tagArray.contains(contentArray.get(i))) {
                      contentTag = contentArray.get(i) :: contentTag
                    }
                  }
                  val skillArray= jieBa.sentenceProcess(skill)
                  for(i <- 0 until skillArray.size()){
                    if(tagArray.contains(skillArray.get(i))){
                      contentTag = skillArray.get(i)::contentTag
                    }
                  }
                  if (!contentTag.isEmpty) {
                    tf = new HashingTF(Math.pow(2, 18).toInt).transform(contentTag)
                  } else tf = null

                } else if ("video".equals(info(6))) {
                  itemType = "6"
                  id = info(1)
                  circleID = info(3)
                  label = info(5)
                  content = info(9)
                  editor = info(12)
                  dept = info(13)
                  skill = info(14)
                  contentTag = List()
                  if (!"".equals(label) && !"\\N".equals(label) && !"null".equalsIgnoreCase(label)) {
                    contentTag = label :: contentTag
                  }
                  if (!"".equals(editor) && !"\\N".equals(editor) && !"null".equalsIgnoreCase(editor)) {
                    contentTag = editor :: contentTag
                  }
                  if (!"".equals(dept) && !"\\N".equals(dept) && !"null".equalsIgnoreCase(dept)) {
                    contentTag = dept :: contentTag
                  }
                  val contentArray = jieBa.sentenceProcess(content)
                  for (i <- 0 until contentArray.size()) {
                    if (tagArray.contains(contentArray.get(i))) {
                      contentTag = contentArray.get(i) :: contentTag
                    }
                  }
                  val skillArray= jieBa.sentenceProcess(skill)
                  for(i <- 0 until skillArray.size()){
                    if(tagArray.contains(skillArray.get(i))){
                      contentTag = skillArray.get(i)::contentTag
                    }
                  }
                  if (!contentTag.isEmpty) {
                    tf = new HashingTF(Math.pow(2, 18).toInt).transform(contentTag)
                  } else tf = null

                } else logger.warn("publishBroadcast中type字段不匹配")
              }

            case "publishDocument" =>
              if (info.length != 8) {
                logger.warn("publishDocument格式不匹配")
              } else {
                itemType = "8"
                circleID = "10000"
                label = info(5)
                contentTag = List()
                if (!"".equals(label) && !"\\N".equals(label) && !"null".equalsIgnoreCase(label)) {
                  contentTag = label :: contentTag
                }
                if (!contentTag.isEmpty) {
                  tf = new HashingTF(Math.pow(2, 18).toInt).transform(contentTag)
                } else tf = null
              }
            case _ => logger.warn("新增内容topic格式不匹配")
          }
          (itemType + "\t" + id + "\t" + circleID, tf)
        })
      }).filter(_._2!=null)

      val tfRDD1 = getTFVectorRDD(rdd.sparkContext).value.persist(StorageLevel.MEMORY_ONLY_SER)
      val idfModel = getIDFModel(rdd.sparkContext, tfRDD1).value
      val bIDF = getIDFVectorArray(rdd.sparkContext, tfRDD1)
      //      val idf = new IDF().fit(contentVector.values)
      val idfRDD1 = tfRDD.mapValues(v => idfModel.transform(v))
      //      val idfRDD2 = contentVector.mapValues(v => idf.transform(v))
      //广播一份tf-idf向量集
      //      val bIDF = rdd.sparkContext.broadcast(idfRDD2.collect())

      val docSims = idfRDD1.flatMap {
        case (id1, idf1) =>
          val idfs = bIDF.value.filter(_._1 != id1)
          val sv1 = idf1.asInstanceOf[SV]
          //构建向量1
          val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
          //取相似度最大的前100个
          idfs.map {
            case (id2, idf2) =>
              val sv2 = idf2.asInstanceOf[SV]
              //构建向量2
              val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
              //计算两向量点乘除以两向量范数得到向量余弦值
              val cosSim = bsv1.dot(bsv2)/(norm(bsv1, 2.0) * norm(bsv2, 2.0))
              (id1, id2, cosSim)

          }.sortWith(_._3>_._3).take(100)
      }


      docSims.foreachPartition(partition=>{
        val redis =  RedisCluster.cluster
        partition.foreach(x=>{
          val info1 = x._1.split("\\t")
          val info2 = x._2.split("\\t")
          redis.rpush(s"CS:${info1(1)}",s"{'crid': '${info2(2)}', 's': ${x._3}, 'type': '${info2(0)}', 'id': '${info2(1)}'}")
        })
//        redis.close()
      })


      //        docSims.foreach(x=>{
      //          println(x._1+"\t"+x._2 +"\t 相似度："+x._3)
      //        })
      //            val idf = idfModel.fit(newSTF.values)
      //            newSTF.mapValues(v => idf.transform(v))
      //      }

    })
  }





  def getContentVector(rdd:RDD[String]):RDD[(String,Vector)]={

    val tagArray = getTagArray(rdd.sparkContext).value
    var tf:Vector = null
    var id:String = null
    var itemType:String = null
    var circleID:String = null
    var editor:String = null
    var dept:String = null
    var label:String = null
    var content:String = null
    var skill:String = null
    var contentTag:List[String] = null
    rdd.mapPartitions(partition=>{
      val jieBa = new JiebaSegmenter()
      partition.map(line=>{
        val info = line.split("\\001",8)
        id = info(0)
        itemType = info(1)
        circleID = info(2)
        editor = info(3)
        dept = info(4)
        label = info(5)
        content = info(6)
        skill = info(7)
        contentTag = List()
        if(!"".equals(editor) && !"\\N".equals(editor) && !"null".equalsIgnoreCase(editor)){
          contentTag = editor::contentTag
        }
        if(!"".equals(dept) && !"\\N".equals(dept) && !"null".equalsIgnoreCase(dept)){
          contentTag = dept::contentTag
        }
        if(!"".equals(label) && !"\\N".equals(label) && !"null".equalsIgnoreCase(label)){
          contentTag = label::contentTag
        }
        val contentArray= jieBa.sentenceProcess(content)

        for(i <- 0 until contentArray.size()){
          if(tagArray.contains(contentArray.get(i))){
            contentTag = contentArray.get(i)::contentTag
          }
        }

        val skillArray= jieBa.sentenceProcess(skill)
        for(i <- 0 until skillArray.size()){
          if(tagArray.contains(skillArray.get(i))){
            contentTag = skillArray.get(i)::contentTag
          }
        }

        if(!contentTag.isEmpty){
          tf = new HashingTF(Math.pow(2, 18).toInt).transform(contentTag)
        }else tf = null
        (itemType + "\t" + id + "\t" + circleID,tf)
      })
    }).filter(_._2 != null)
  }

  def convertTimeStamp2DateStr(timestamp: Long, pattern: String): String = {
    val format = new SimpleDateFormat(pattern)
    format.format(new Date(timestamp))
  }

  def getYesterday():String= {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val yesterday = dateFormat.format(cal.getTime())
    yesterday
  }

  def getTomorrow():String= {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, 1)
    val tomorrow = dateFormat.format(cal.getTime())
    tomorrow
  }
}
