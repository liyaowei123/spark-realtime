package edu.lyw.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
/**
 * Redis连接工具类
 * */
object MyRedisUtil {
  var jedisPool: JedisPool = null
  /**
   * 获取Redis连接
   * */
  def getJedisPoolFromPoll(): Jedis = {
    if (jedisPool == null) {
      // 创建连接池对象
      // 连接池配置
      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100)
      jedisPoolConfig.setMaxIdle(20)
      jedisPoolConfig.setMinIdle(20)
      jedisPoolConfig.setBlockWhenExhausted(true)
      jedisPoolConfig.setMaxWaitMillis(5000)
      jedisPoolConfig.setTestOnBorrow(true)

      val host = MyPropertiesUtil(MyConfig.REDIS_HOST)
      val port = MyPropertiesUtil(MyConfig.REDIS_PORT)

      jedisPool = new JedisPool(jedisPoolConfig, host, port.toInt)
    }
    jedisPool.getResource
  }
}
