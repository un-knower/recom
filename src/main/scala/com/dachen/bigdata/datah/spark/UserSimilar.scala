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

object UserSimilar {
  private val logger = Logger.getLogger(UserSimilar.getClass)
  private val conf = new SparkConf().setAppName("userSimilar").setMaster("yarn-client")
  private val spark = new SparkContext(conf)
  spark.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      logger.warn(
        """
          |The parameter length must be three
          |The first parameter is the word library path
          |The second parameter is the tag library path
          |The third parameter is the latest path of the user hive
        """.stripMargin)
    } else {
      val lexiconPath = args(0)
      val tagPath = args(1)
      val userPath = args(2)
      val toPath = FileSystems.getDefault.getPath(lexiconPath)
      //使用结巴分词，加载自己的词库
      WordDictionary.getInstance().loadUserDict(toPath)
      getSimilar(tagPath, userPath)
    }

  }

  def getSimilar(tagPath: String, userPath: String): Unit= {
    logger.warn("计算用户相似度程序开始执行……")
    val tfRDD = getUserVectorRDD(tagPath: String, userPath: String).persist(StorageLevel.MEMORY_ONLY_SER)
    val idf = new IDF().fit(tfRDD.values)
    val idfRDD = tfRDD.mapValues(v => idf.transform(v))
    val bIDF = spark.broadcast(idfRDD.collect())
    val docSims = idfRDD.flatMap {
      case (id1, idf1) =>
        val idfs = bIDF.value.filter(_._1 != id1)
        val sv1 = idf1.asInstanceOf[SV]
        //构建向量1
        val bsv1 = new SparseVector[Double](sv1.indices, sv1.values, sv1.size)
        //取相似度最大的前50个
        idfs.map {
          case (id2, idf2) =>
            val sv2 = idf2.asInstanceOf[SV]
            //构建向量2
            val bsv2 = new SparseVector[Double](sv2.indices, sv2.values, sv2.size)
            //计算两向量点乘除以两向量范数得到向量余弦值
            val cosSim = bsv1.dot(bsv2) / (norm(bsv1, 2.0) * norm(bsv2, 2.0))
            (id1, id2, cosSim)

        }.sortWith(_._3 > _._3).take(50)
    }

    val pairRDD = docSims.map(x => {
      //      println(x._3)
      (x._1, x._2 + "\t" + x._3)
    }).reduceByKey((x1, x2) => {
      x1 + "|" + x2
    })
    var i: Int = 0
    pairRDD.foreachPartition(partition => {
      val redis = RedisCluster.cluster
//      val pipe = redis.pipelined()
      partition.foreach(x => {
        val info1 = x._2.split("\\|")
        redis.del(s"US:${x._1}")
        for (j <- 0 until info1.length) {
          val info3 = info1(j).split("\\t")
          //            logger.warn(info2(j))
          redis.rpush(s"US:${x._1}", s"{'id': '${info3(0)}', 's': ${info3(1)}}")
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
    println("程序执行结束")
  }


  def getUserVectorRDD(tagPath: String, userPath: String): RDD[(String, Vector)] = {

    val tagArray = spark.textFile(tagPath).collect()
    val textRDD = spark.textFile(userPath, 60)
    var tf: Vector = null
    var id: String = null
    var dept: String = null
    var hospital: String = null
    var focusDept: String = null
    var focusDoc: String = null
    var focusTopic: String = null
    var skill: String = null
    var contentTag: List[String] = null

    textRDD.mapPartitions(partition => {
      val jieBa = new JiebaSegmenter()
      partition.map(line => {
        val info = line.split("\\001", 7)
        id = info(0)
        dept = info(1)
        hospital = info(2)
        focusDept = info(3)
        focusDoc = info(4)
        focusTopic = info(5)
        skill = info(6)
        contentTag = List()
        if (!"".equals(dept) && !"\\N".equals(dept) && !"null".equalsIgnoreCase(dept)) {
          contentTag = dept :: contentTag
        }
        if (!"".equals(hospital) && !"\\N".equals(hospital) && !"null".equalsIgnoreCase(hospital)) {
          contentTag = hospital :: contentTag
        }
        if (!"".equals(focusDept) && !"\\N".equals(focusDept) && !"null".equalsIgnoreCase(focusDept)) {
          contentTag = focusDept :: contentTag
        }
        if (!"".equals(focusDoc) && !"\\N".equals(focusDoc) && !"null".equalsIgnoreCase(focusDoc)) {
          contentTag = focusDoc :: contentTag
        }
        if (!"".equals(focusTopic) && !"\\N".equals(focusTopic) && !"null".equalsIgnoreCase(focusTopic)) {
          contentTag = focusTopic :: contentTag
        }
        val contentArray = jieBa.sentenceProcess(skill)
        for (i <- 0 until contentArray.size()) {
          if (tagArray.contains(contentArray.get(i))) {
            contentTag = contentArray.get(i) :: contentTag
          }
        }
        if (!contentTag.isEmpty) {
          tf = new HashingTF(Math.pow(2, 18).toInt).transform(contentTag)
        } else tf = null
        (id, tf)
      })
    }).filter(_._2 != null)
  }
}