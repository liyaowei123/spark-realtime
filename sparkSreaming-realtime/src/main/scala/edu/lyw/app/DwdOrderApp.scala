package edu.lyw.app

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import edu.lyw.bean.{OrderDetail, OrderInfo, OrderWide}
import edu.lyw.utils.{MyEsUtil, MyKafkaUtil, MyOffsetUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.time.{LocalDate, Period}
import scala.collection.mutable.ListBuffer

/**
 * 订单宽表任务
 * 1.准备实时环境
 * 2.从redis读取offset
 * 3.消费数据
 * 4.从kafka提取offset
 * 5.处理数据
 * 6.写入ES
 * 7.提交offset
 * */
object DwdOrderApp {
  def main(args: Array[String]): Unit = {
    // 1.实时环境
    val conf = new SparkConf().setMaster("local[4]").setAppName("dwd_order_app")
    val ssc = new StreamingContext(conf, Seconds(5))

    // 2。从redis读取offset
    val orderInfoTopicName = "DWD_ORDER_INFO_I"
    val orderInfoGroupId = "DWD_ORDER_INFO:GROUP"
    val orderInfoOffset = MyOffsetUtil.readOffset(orderInfoTopicName, orderInfoGroupId)

    val orderDetailTopicName = "DWD_ORDER_DETAIL_I"
    val orderDetailGroupId = "DWD_ORDER_DETAIL:GROUP"
    val orderDetailOffset = MyOffsetUtil.readOffset(orderDetailTopicName, orderDetailGroupId)
    // 3.消费数据
    var orderInfoKafkaDStream: DStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffset != null && orderInfoOffset.nonEmpty) {
      orderInfoKafkaDStream = MyKafkaUtil.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroupId, orderInfoOffset)
    } else {
      orderInfoKafkaDStream = MyKafkaUtil.getKafkaDStream(ssc, orderInfoTopicName, orderInfoGroupId)
    }
    var orderDetailKafkaDStream: DStream[ConsumerRecord[String, String]] = null
    if (orderInfoOffset != null && orderInfoOffset.nonEmpty) {
      orderDetailKafkaDStream = MyKafkaUtil.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroupId, orderDetailOffset)
    } else {
      orderDetailKafkaDStream = MyKafkaUtil.getKafkaDStream(ssc, orderDetailTopicName, orderDetailGroupId)
    }
    // 4.从kafka提取offset
    var orderInfoRangeOffsets: Array[OffsetRange] = null
    orderInfoKafkaDStream = orderInfoKafkaDStream.transform(
      rdd => {
        orderInfoRangeOffsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    var orderDetailRangeOffsets: Array[OffsetRange] = null
    orderDetailKafkaDStream = orderDetailKafkaDStream.transform(
      rdd => {
        orderDetailRangeOffsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    // 5.处理数据
    // 5.1转换数据结构
    val orderInfoDStream = orderInfoKafkaDStream.map(
      consumerRecord => {
        val value = consumerRecord.value

        val orderInfo = JSON.parseObject(value, classOf[OrderInfo])
        orderInfo
      }
    )

    val orderDetailDStream = orderDetailKafkaDStream.map(
      consumerRecord => {
        val value = consumerRecord.value
        val orderDetail = JSON.parseObject(value, classOf[OrderDetail])
        orderDetail
      }
    )

    val orderInfoDimDStream = orderInfoDStream.mapPartitions(
      orderInfoIter => {
        val orderInfos = new ListBuffer[OrderInfo]
        val jedis = MyRedisUtil.getJedisPoolFromPoll()
        for (orderInfo <- orderInfoIter) {
          // 关联用户维度
          val str = jedis.get(s"DIM:USER_INFO:${orderInfo.user_id}")

          val userInfoJson = JSON.parseObject(str)
          orderInfo.user_gender = userInfoJson.getString("gender")
          orderInfo.user_age = Period.between(LocalDate.parse(userInfoJson.getString("birthday")), LocalDate.now).getYears
          // 关联地区维度
          val provinceJson = JSON.parseObject(jedis.get(s"DIM:BASE_PROVINCE:${orderInfo.province_id}"))
          orderInfo.province_name = provinceJson.getString("name")
          orderInfo.province_area_code = provinceJson.getString("area_code")
          orderInfo.province_3166_2_code = provinceJson.getString("iso_3166_2")
          orderInfo.province_iso_code = provinceJson.getString("iso_code")
          // 日期字段
          val createTime = orderInfo.create_time
          val createDtHr = createTime.split(" ")
          val createDate = createDtHr(0)
          val createHr = createDtHr(1).split(":")(0)
          orderInfo.create_date = createDate
          orderInfo.create_hour = createHr

          orderInfos.append(orderInfo)
        }
        jedis.close()
        orderInfos.iterator
      }
    )
    val orderInfoKV = orderInfoDimDStream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailKV = orderDetailDStream.map(orderDetail => (orderDetail.order_id, orderDetail))
    val orderJoinDStream = orderInfoKV.fullOuterJoin(orderDetailKV)

    val orderWideDStream = orderJoinDStream.mapPartitions(
      orderJoinIter => {
        val jedis = MyRedisUtil.getJedisPoolFromPoll()
        val orderWides = new ListBuffer[OrderWide]
        for ((key, (orderInfoOp, orderDetailOp)) <- orderJoinIter) {
          // orderInfo有，orderDetail有
          if (orderInfoOp.isDefined) {
            val orderInfo = orderInfoOp.get
            if (orderDetailOp.isDefined) {
              val orderDetail = orderDetailOp.get
              val orderWide = new OrderWide(orderInfo, orderDetail)
              orderWides.append(orderWide)
            }

            // orderInfo有，orderDetail没有
            // orderInfo写缓存
            val redisOrderInfoKey = s"ORDERJOIN:REDER_INFO:${orderInfo.id}"
            val orderInfoJson = JSON.toJSONString(orderInfo, new SerializeConfig(true))
            jedis.setex(redisOrderInfoKey, 24 * 3600, orderInfoJson)

            // orderInfo读缓存
            val redisOrderDetailKey = s"ORDERJOIN:ORDER_DETAIL:${orderInfo.id}"
            val orderDetails = jedis.smembers(redisOrderDetailKey)
            if (orderDetails != null && orderDetails.size > 0) {
              import scala.collection.JavaConverters._
              for (orderDetailJson <- orderDetails.asScala) {
                val orderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                val orderWide = new OrderWide(orderInfo, orderDetail)
                orderWides.append(orderWide)
              }
            }
          } else {
            // orderInfo没有，orderDetail有
            val orderDetail = orderDetailOp.get
            // 读缓存
            val redisOrderInfoKey = s"ORDERJOIN:ORDER_INFO:${orderDetail.order_id}"
            val orderInfoJson = jedis.get(redisOrderInfoKey)
            if (orderInfoJson != null && orderInfoJson.size > 0) {
              val orderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
              val orderWide = new OrderWide(orderInfo, orderDetail)
              orderWides.append(orderWide)
            } else {
              val redisOrderDetailKey = s"ORDERJOIN:ORDER_DETAIL:${orderDetail.order_id}"
              jedis.sadd(redisOrderDetailKey, JSON.toJSONString(orderDetail, new SerializeConfig(true)))
              jedis.expire(redisOrderDetailKey, 24 * 3600)
            }
          }
        }
        jedis.close()
        orderWides.iterator
      }
    )
    // orderWideDStream.print(100)
    // 6.写入ES
    orderWideDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          orderWideIter => {
            val orderWides: List[(String, OrderWide)] = orderWideIter.map(orderWide => (orderWide.detail_id.toString, orderWide)).toList
            if (orderWides.size > 0) {
              val head: (String, OrderWide) = orderWides.head
              val create_date: String = head._2.create_date
              val indexName = s"gmall_order_wide_$create_date"
              MyEsUtil.bulkSave(indexName, orderWides)
            }
          }
        )
      }
    )
    // 7.提交offset
    MyOffsetUtil.saveOffset(orderInfoTopicName, orderInfoGroupId, orderInfoRangeOffsets)
    MyOffsetUtil.saveOffset(orderDetailTopicName, orderDetailGroupId, orderDetailRangeOffsets)

    ssc.start
    ssc.awaitTermination
  }


}
