package com.dachen.bigdata.datah.spark.util

import java.util.Properties

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object RedisUtil extends Serializable {

  private val pro = new Properties()
  pro.load(this.getClass.getResourceAsStream("/test_redis.properties"))
  val redisHost = pro.getProperty("redisHost")
  val redisPort = pro.getProperty("redisPort").toInt
  val redisTimeout = pro.getProperty("redisTimeout").toInt
  val password = pro.getProperty("password")
  val dbIndex = pro.getProperty("dbIndex").toInt
  lazy val pool = new JedisPool(new GenericObjectPoolConfig(), redisHost, redisPort, redisTimeout, password, dbIndex)
  lazy val hook = new Thread {
    override
    def run = {
      println("Execute hook thread: " + this)
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook.run)
}