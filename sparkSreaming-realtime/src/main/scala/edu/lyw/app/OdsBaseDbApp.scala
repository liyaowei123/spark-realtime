package edu.lyw.app

import edu.lyw.utils.{MyKafkaUtil, MyOffsetUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.alibaba.fastjson._
/**
 * 业务数据消费分流
 *
 * 1.准备实时环境
 *
 * 2.从redis中读取偏移量
 *
 * 3.从kafka中消费数据
 *
 * 4.提取偏移量结束点
 *
 * 5.数据处理
 *    5.1转换数据结构
 *    5.2分流
 *      事实数据 => kafka
 *      维度数据 => redis
 *
 * 6.刷写缓冲区
 *
 * 7.提交offset
 * */
object OdsBaseDbApp {
  def main(args: Array[String]): Unit = {
    // 1.准备实时环境
    val conf = new SparkConf().setMaster("local[4]").setAppName("ods_base_db_app")
    val ssc = new StreamingContext(conf, Seconds(5))

    val topic: String = "ODS_BASE_DB"
    val group: String = "ODS_BASE_DB_GROUP"

    // 2.从redis中读取偏移量
    val offsets: Map[TopicPartition, Long] = MyOffsetUtil.readOffset(topic, group)

    // 3.从kafka中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = MyKafkaUtil.getKafkaDStream(ssc, topic, group, offsets)
    } else {
      kafkaDStream = MyKafkaUtil.getKafkaDStream(ssc, topic, group)
    }

    // 4.提取偏移量结束点
    var offsetRanges:Array[OffsetRange] = null
    val offsetRangesDStream = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 5.数据处理
    val record = offsetRangesDStream.map(
      consumerRecord => {
        val value = consumerRecord.value
        val jsonObject = JSON.parseObject(value)
        jsonObject
      }
    )
    record.foreachRDD(
      rdd => {
        // 事实表清单
        val redisFactKey = "FACT:TABLES"
        val tableJedis = MyRedisUtil.getJedisPoolFromPoll()
        val factTables = tableJedis.smembers(redisFactKey)
        val factTablesBC = ssc.sparkContext.broadcast(factTables)
        // 维度表清单
        val redisDimKey = "DIM:TABLES"
        val dimTables = tableJedis.smembers(redisDimKey)
        val dimTablesBC = ssc.sparkContext.broadcast(dimTables)

        println("dim tables:" + dimTablesBC.value)
        println("dim tables:" + factTablesBC.value)

        tableJedis.close()
        rdd.foreachPartition(
          jsonObjectIter => {
            val jedis = MyRedisUtil.getJedisPoolFromPoll()
            for (jsonObject <- jsonObjectIter) {
              // 提取操作类型
              val operateType = jsonObject.getString("type")
              val operateTypeChar = operateType match {
                case "bootstrap-insert" => "I"
                case "insert" => "I"
                case "update" => "U"
                case "delete" => "D"
                case _ => null
              }
              if (operateTypeChar != null) {
                // 提取表名
                val tableName: String = jsonObject.getString("table")

                if (tableName.equals("base_province")) {
                  println("table=" + tableName)

                }

                if (factTablesBC.value.contains(tableName)) {
                  val data: String = jsonObject.getString("data")
                  // 事实数据
                  val topicName = s"DWD_${tableName.toUpperCase}_${operateTypeChar}"
                  MyKafkaUtil.send(topicName, data)
                  println("DATA:" + data)
                }

                if (dimTablesBC.value.contains(tableName)) {
                  // 维度数据
                  val data = jsonObject.getJSONObject("data")
                  val id = data.getString("id")
                  val redisKey = s"DIM:${tableName.toUpperCase}:$id"
                  jedis.set(redisKey, data.toJSONString)
                }
              }
            }
            jedis.close()
            MyKafkaUtil.flush
          }
        )
        MyOffsetUtil.saveOffset(topic, group, offsetRanges)
      }
    )
    ssc.start
    ssc.awaitTermination
  }
}
