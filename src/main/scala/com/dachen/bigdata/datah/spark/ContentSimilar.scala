package com.dachen.bigdata.datah.spark

import java.nio.file.FileSystems

import breeze.linalg.{SparseVector, norm}
import com.dachen.bigdata.datah.spark.util.{RedisCluster, RedisUtil}
import com.huaban.analysis.jieba.{JiebaSegmenter, WordDictionary}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{Vector, SparseVector => SV}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object ContentSimilar {
  private val logger = Logger.getLogger(ContentSimilar.getClass)
  private val conf = new SparkConf().setAppName("contentSimilar").setMaster("yarn-client")
  private val spark = new SparkContext(conf)
  spark.setLogLevel("WARN")
  def main(args: Array[String]): Unit = {
    if(args.length != 3){
      logger.warn(
        """
          |The parameter length must be three
          |The first parameter is the word library path
          |The second parameter is the tag library path
          |The third parameter is the latest path of the content hive
        """.stripMargin)
    }else{
      val lexiconPath = args(0)
      val tagPath = args(1)
      val contentPath = args(2)
      val toPath = FileSystems.getDefault.getPath(lexiconPath)
      //使用结巴分词，加载自己的词库
      WordDictionary.getInstance().loadUserDict(toPath)
      getSimilar(tagPath,contentPath)
    }
  }

  def getSimilar(tagPath:String, contentPath:String): Unit = {
    logger.warn("计算内容相似度程序开始执行……")
    val tfRDD = getContentVector(tagPath, contentPath).persist(StorageLevel.MEMORY_ONLY_SER)
    val idf = new IDF().fit(tfRDD.values)
    val idfRDD = tfRDD.mapValues(v => idf.transform(v))
    val bIDF = spark.broadcast(idfRDD.collect())
    val docSims = idfRDD.flatMap {
      case (id1, idf1) =>
        val idfs = bIDF.value.filter(_._1!= id1)
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
    val pairRDD = docSims.map(x=>{
      (x._1,x._2 + "\t" + x._3)
    }).reduceByKey((x1, x2)=>{
      x1 + "|" + x2
    })
    logger.warn("开始写入redis……")
    var i:Int = 0
    pairRDD.foreachPartition(partition=>{
      val redis =  RedisCluster.cluster

//      val pipe = redis.pipelined()
      partition.foreach(x => {
        val info1 = x._1.split("\\t")
        val info2 = x._2.split("\\|")
        redis.del(s"CS:${info1(1)}")
        for(j <- 0 until info2.length){
          val info3 = info2(j).split("\\t")
          redis.rpush(s"CS:${info1(1)}", s"{'crid': '${info3(2)}', 's': ${info3(3)}, 'type': '${info3(0)}', 'id': '${info3(1)}'}")
//          i = i + 1
//          if (i > 50000) {
//            pipe.sync()
//            i = 0
//          }
        }
      })
//      redis.close()
//      pipe.sync()
    })
    logger.warn("程序执行结束")
  }

  def getContentVector(tagPath:String, contentPath:String): RDD[(String, Vector)] = {

    val tagArray = spark.textFile(tagPath).collect()
    val textRDD = spark.textFile(contentPath, 60)
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
    textRDD.mapPartitions(partition => {
      val jieBa = new JiebaSegmenter()
      partition.map(line => {
        val info = line.split("\\001", 8)
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
        if(!contentArray.isEmpty){
          for(i <- 0 until contentArray.size()){
            if(tagArray.contains(contentArray.get(i))){
              contentTag = contentArray.get(i)::contentTag
            }
          }
        }

        val skillArray= jieBa.sentenceProcess(skill)
        if(!skillArray.isEmpty){
          for(i <- 0 until skillArray.size()){
            if(tagArray.contains(skillArray.get(i))){
              contentTag = skillArray.get(i)::contentTag
            }
          }
        }

        if(!contentTag.isEmpty){
          tf = new HashingTF(Math.pow(2, 18).toInt).transform(contentTag)
        }else tf = null

        (itemType + "\t" + id + "\t" + circleID, tf)
      })
    }).filter(_._2 != null)
  }
}
