package com.dachen.bigdata.datah.spark.util

import java.util.Properties

import redis.clients.jedis.{HostAndPort, JedisCluster, JedisShardInfo, ShardedJedisPool}
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import java.util
object RedisCluster extends Serializable {

  private val pro = new Properties()
  pro.load(this.getClass.getResourceAsStream("/uat_redis.properties"))
  val hostPort = pro.getProperty("hostPort").split(",").toSet
  val password = pro.getProperty("password")
  val maxTotal = pro.getProperty("maxTotal").toInt
  val maxIdle = pro.getProperty("maxIdle").toInt
  val minIdle = pro.getProperty("minIdle").toInt
  val maxWaitMillis = pro.getProperty("maxWaitMillis").toInt
  val testOnBorrow = pro.getProperty("testOnBorrow").toBoolean
  val testOnReturn = pro.getProperty("testOnReturn").toBoolean

  val shards = new util.ArrayList[JedisShardInfo]()
  for(string <- hostPort){
    val info = string.split(":")
    val shard=new JedisShardInfo(info(0),Integer.parseInt(info(1)))
    shard.setPassword(password)
    shards.add(shard)
  }

  val nodes = new util.HashSet[HostAndPort]()
for(string <- hostPort){
  val info = string.split(":")
  val hostAndPort = new HostAndPort(info(0), info(1).toInt)
  nodes.add(hostAndPort)
}



  val config = new GenericObjectPoolConfig
  config.setMaxTotal(maxTotal)
  config.setMaxIdle(maxIdle)
  config.setMinIdle(minIdle)
  config.setMaxWaitMillis(maxWaitMillis)
  config.setTestOnBorrow(testOnBorrow)
  config.setTestOnReturn(testOnReturn)

 val cluster = new JedisCluster(nodes,5000,5000,10,password,config)

  val nodeMap = cluster.getClusterNodes()

  val anyHost = nodeMap.keySet().iterator().next()
  val slotHostMap = getSlotHostMap(anyHost)

//  lazy val shardedJedisPool = new ShardedJedisPool(config, shards)

//  lazy val hook = new Thread {
//    override
//    def run = {
//      println("Execute hook thread: " + this)
//      cluster.destroy()
//    }
//  }

  import redis.clients.jedis.Jedis

  private def getSlotHostMap(anyHostAndPortStr: String) = {
    val tree = new util.TreeMap[Long,String]()
    val parts = anyHostAndPortStr.split(":")
    val anyHostAndPort = new HostAndPort(parts(0), parts(1).toInt)
    try {
      val jedis = new Jedis(anyHostAndPort.getHost, anyHostAndPort.getPort)
      val list:util.List[Object] = jedis.clusterSlots
      import scala.collection.JavaConversions._
      for (cluster <- list) {
        val list1 = cluster.asInstanceOf[util.List[Object]]
        val master = list1.get(2).asInstanceOf[List[Object]]
        val hostAndPort = new String(master.get(0).asInstanceOf[Array[Byte]]) + ":" + master.get(1)
        tree.put(list1.get(0).asInstanceOf[Long], hostAndPort)
        tree.put(list1.get(1).asInstanceOf[Long], hostAndPort)
      }
      jedis.close()
    } catch {
      case e: Exception =>
    }
    tree
  }


//  sys.addShutdownHook(hook.run)
}
