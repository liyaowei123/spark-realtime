package edu.lyw.utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis

import java.util
import scala.collection.mutable

/**
 * Offset管理工具类
 * 用于向redis读取和存储offset
 *
 * 管理方案：
 *  1.后置提交偏移量 -> 手动控制偏移量提交
 *  2.手动控制偏移量提交 -> spark streaming提供了手动提交方案，但是我们不能用，因为我们会对DStream进行转换
 *  3.手动提取偏移量维护到redis中：
 *                            1）从Kafka中消费数据的时候先提取偏移量
 *                            2）等数据成功写出之后，将偏移量存储到redis中
 *                            3）从kafka消费数据之前，先到redis中读取偏移量，使用读取到的偏移量到kafka中消费数据
 * */
object MyOffsetUtil {
  /**
   * 向redis存储offset
   *
   * 问题： 存的offset从哪里来？
   *          从kafka中消费到的数据提取，传入到该方法之中。
   *          offsetRanges: Array[offsetRange]
   *
   *       offset的结构？
   *          kafka中维护的offset的结构
   *            groupId + topic + partition => offset
   *
   *       在redis中如何存储？
   *          类型：hash
   *          key：groupId + topic
   *          value：partition - offset
   * */
  def saveOffset(topic: String, groupId: String, offsetRanges: Array[OffsetRange]) = {
    println("======================saveOffset=====================")
    // 创建一个map作为redis的value
    val redisValue = new util.HashMap[String, String]()
    if (offsetRanges != null && offsetRanges.length > 0) {
      for (offsetRange <- offsetRanges) {
        // 获取partition
        val partition = offsetRange.partition
        // 获取offset
        val endOffset = offsetRange.untilOffset
        // 写入map
        redisValue.put(partition.toString, endOffset.toString)
      }
      println("保存offset:" + redisValue)
      // redis的key
      val redisKey = s"offset:$topic:$groupId"
      // 获取redis连接对象
      val jedis = MyRedisUtil.getJedisPoolFromPoll()
      // 写入redis
      jedis.hset(redisKey, redisValue)
      jedis.close()
    }
  }

  /**
   * 从redis读取offset
   *
   * 问题：
   *      如何让spark streaming通过指定的offset进行消费？
   *
   *      spark streaming要求的offset的格式是什么？
   *        Map[TopicPartition, Long]
   * */
  def readOffset(topic: String, groupId: String): Map[TopicPartition, Long] = {
    println("======================readOffset=====================")
    // 获取redis连接
    val jedis: Jedis = MyRedisUtil.getJedisPoolFromPoll()
    // 定义key值
    val redisKey = s"offsets:$topic:$groupId"
    // 根据key值获取全部value
    val offsets: util.Map[String, String] = jedis.hgetAll(redisKey)
    val results: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()
    // 将Java的map转换成Scala的map进行迭代
    import scala.collection.JavaConverters._
    for ((partition, offset) <- offsets.asScala) {
      // 根据传入进来的参数topic和由Java的map转换而来的partition创建一个TopicPartition对象
      val topicPartition = new TopicPartition(topic, partition.toInt)
      // 将topicPartition和offset放入results
      results.put(topicPartition, offset.toLong)
      println("读取offset:" + offset)
    }

    jedis.close()
    results.toMap
  }
}
