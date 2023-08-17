package edu.lyw.app

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import edu.lyw.utils.{MyKafkaUtil, MyOffsetUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import edu.lyw.bean.PageLog
import edu.lyw.bean.PageActionLog
import edu.lyw.bean.PageDisplayLog
import edu.lyw.bean.StartLog
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
 * 日志数据的消费分流
 * 1.准备实时处理环境
 *
 * 2.从kafka中消费数据
 *
 * 3.处理数据
 *    3.1转换数据结构
 *    3.2分流
 *
 * 3.写出到DWD层
 * */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    // 创建实时环境
    // 并行度与Kafka分区的数量一致
    // 每5秒采集一次
    val sc = new SparkConf().setMaster("local[4]").setAppName("ods_base_log_app")
    val ssc = new StreamingContext(sc, Seconds(5))
    // Kafka主题
    val topicName = "ODS_BASE_LOG"
    // Kafka消费者组
    val groupId = "ODS_BASE_LOG_GROUP"
    // 获取offset
    val offsets = MyOffsetUtil.readOffset(topicName, groupId)

    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      // 指定offset进行消费
      kafkaDStream = MyKafkaUtil.getKafkaDStream(ssc, topicName, groupId, offsets)
    } else {
      // 使用Kafka默认的offset消费数据
      kafkaDStream = MyKafkaUtil.getKafkaDStream(ssc, topicName, groupId)
    }

    // TODO 从消费到的数据中提取offsets，不对流中的数据做任何处理。
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    // 将从Kafka中消费到的数据ConsumerRecord进行处理，利用map算子转换成json数据结构，然后打印输出
    val jsonObjectDStream = offsetRangesDStream.map(
      // 这是一个lambda表达式
      consumerRecord => {
        // 获取ConsumerRecord的value值
        val log = consumerRecord.value()
        // 将获取到的value值封装成JSON数据
        val jSONObject = JSON.parseObject(log)
        // 返回JSON
        jSONObject
      }
    )
    // 打印数据，因为数据在打印完一次之后便不在保存，因此只在测试时使用
    // jsonObjectDStream.print(100)

    // 定义分流主题
    val DWD_PAGE_LOG_TOPIC: String = "DWD_PAGE_LOG_TOPIC"         // 页面访问
    val DWD_PAGE_DISPLAY_TOPIC: String = "DWD_PAGE_DISPLAY_TOPIC" // 页面曝光
    val DWD_PAGE_ACTION_TOPIC: String = "DWD_PAGE_ACTION_TOPIC"   // 页面动作
    val DWD_START_LOG_TOPIC: String = "DWD_START_LOG_TOPIC"       // 启动数据
    val DWD_ERROR_LOG_TOPIC: String = "DWD_ERROR_LOG_TOPIC"       // 错误数据

    jsonObjectDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          jsonObjIter => {
            for (jsonObj <- jsonObjIter) {
                // 分流错误数据
                val err = jsonObj.getString("err")
                if (err != null) {
                  // 如果错误数据不为空，则发送到DWD_ERROR_LOG_TOPIC主题
                  MyKafkaUtil.send(DWD_ERROR_LOG_TOPIC, jsonObj.toJSONString)
                } else {
                  // 提取公共字段
                  val commonObj = jsonObj.getJSONObject("common")
                  val ar = commonObj.getString("ar")
                  val uid = commonObj.getString("uid")
                  val os = commonObj.getString("os")
                  val ch = commonObj.getString("ch")
                  val isNew = commonObj.getString("is_new")
                  val md = commonObj.getString("md")
                  val mid = commonObj.getString("mid")
                  val vc = commonObj.getString("vc")
                  val ba = commonObj.getString("ba")
                  // 提取时间戳
                  val ts = jsonObj.getLong("ts")
                  // 页面数据
                  val pageObj = jsonObj.getJSONObject("page")
                  if (pageObj != null) {
                    // 提取page字段
                    val pageId = pageObj.getString("page_id")
                    val pageItem = pageObj.getString("item")
                    val pageItemType = pageObj.getString("item_type")
                    val duringTime = pageObj.getLong("during_time")
                    val lastPageId = pageObj.getString("last_page_id")
                    val sourceType = pageObj.getString("source_type")
                    // 封装PageLog对象
                    val pageLog = PageLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, ts)
                    // 发送数据到DWD_PAGE_LOG_TOPIC
                    MyKafkaUtil.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))

                    // 提取曝光数据
                    val displayArr = jsonObj.getJSONArray("displays")
                    if (displayArr != null && displayArr.size() > 0) {
                      for (i <- 0 until displayArr.size()) {
                        val displayObj = displayArr.getJSONObject(i)
                        val displayType = displayObj.getString("display_type")
                        val displayItem = displayObj.getString("item")
                        val displayItemType = displayObj.getString("item_type")
                        val order = displayObj.getString("order")
                        val displayPosId = displayObj.getString("pos_id")
                        // 封装PageDisplayLog对象
                        val displayLog = PageDisplayLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, displayType, displayItem, displayItemType, order, displayPosId, ts)
                        // 发送数据到DWD_PAGE_DISPLAY_TOPIC
                        MyKafkaUtil.send(DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(displayLog, new SerializeConfig(true)))
                      }
                    }

                    //提取行为数据
                    val actionArr = jsonObj.getJSONArray("actions")
                    if (actionArr != null && actionArr.size() > 0) {
                      for (i <- 0 until actionArr.size()) {
                        val actionObj = actionArr.getJSONObject(i)
                        val actionId = actionObj.getString("action_id")
                        val actionItem = actionObj.getString("item")
                        val actionItemType = actionObj.getString("item_type")
                        // 封装PageActionLog
                        val actionLog = PageActionLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, actionId, actionItem, actionItemType, ts)
                        // 发送数据到DWD_PAGE_ACTION_TOPIC
                        MyKafkaUtil.send(DWD_PAGE_ACTION_TOPIC, JSON.toJSONString(actionLog, new SerializeConfig(true)))
                      }
                    }
                  }
                  // 启动数据
                  val startObj = jsonObj.getJSONObject("start")
                  if (startObj != null) {
                    val startEntry = startObj.getString("entry")
                    val startLoadTimeMs = startObj.getLong("loading_time_ms")
                    val startOpenAdId = startObj.getString("open_ad_id")
                    val startOpenAdMs = startObj.getLong("open_ad_ms")
                    val startOpenAdSkipMs = startObj.getLong("open_ad_skip_ms")
                    // 封装startLog
                    val startLog = StartLog(mid, uid, ar, ch, isNew, md, os, vc, startEntry, startOpenAdId, startLoadTimeMs, startOpenAdMs, startOpenAdSkipMs, ts)
                    // 发送数据到DWD_START_LOG_TOPIC
                    MyKafkaUtil.send(DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, new SerializeConfig(true)))
                  }
                }
              }
            // 刷新数据到Kafka
            MyKafkaUtil.flush
          }
        )

        // 保存偏移量
        MyOffsetUtil.saveOffset(topicName, groupId, offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}