package edu.lyw.app

import com.alibaba.fastjson.JSON
import edu.lyw.bean.{DauInfo, PageLog}
import edu.lyw.utils.{MyBeanUtil, MyEsUtil, MyKafkaUtil, MyOffsetUtil, MyRedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.util.Date
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSON.headOptionTailToFunList

/**
 * 日活宽表
 *
 * 1.准备实时环境
 * 2.从redis中读取偏移量
 * 3.从kafka中消费数据
 * 4.提取偏移量结束点
 * 5.处理数据
 *  5.1转换数据结构
 *  5.2去重
 *  5.3维度关联
 * 6.写入ES
 * 7.提交offset
 * */
object DwdDauApp {
  def main(args: Array[String]): Unit = {
    revertStart()
    // 1.处理实时数据
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("dwd_dau_app")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 2.从redis中读取偏移量
    val topicName = "DWD_PAGE_LOG_TOPIC"
    val groupId = "DWD_DAU_GROUP"
    val offsets = MyOffsetUtil.readOffset(topicName, groupId)

    // 3.从kafka中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = MyKafkaUtil.getKafkaDStream(ssc, topicName, groupId, offsets)
    } else {
      kafkaDStream = MyKafkaUtil.getKafkaDStream(ssc, topicName, groupId)
    }

    // 4.提取offset结束点
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    // 5.处理数据
    val pageLogDStream = offsetRangesDStream.map(
      consumerRecord => {
        val str = consumerRecord.value()
        val pageLog = JSON.parseObject(str, classOf[PageLog])
        pageLog
      }
    )
    pageLogDStream.cache()
    pageLogDStream.foreachRDD(
      rdd => {
        println("自我审查前：" + rdd.count())
      }
    )
    // pageLogDStream.print(1000)
    // 数据去重
    //    自我审查：将页面访问数据中last_page_id不为空的数据全部过滤掉
    val filterDStream = pageLogDStream.filter(
      pageLog => pageLog.last_page_id == null
    )

    filterDStream.foreachRDD(
      rdd => {
        println("自我审查前：" + rdd.count())
        println("----------------------------")
      }
    )

    // 第三方审查
    val redisFilterDStream = filterDStream.mapPartitions(
      pageLogIter => {
        val pageLogList = pageLogIter.toList
        println("第三方审查前：" + pageLogList.size)
        val pageLogs: ListBuffer[PageLog] = ListBuffer[PageLog]()
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val jedis = MyRedisUtil.getJedisPoolFromPoll()
        for (pageLog <- pageLogList) {
          val ts = pageLog.ts
          val dateStr = sdf.format(new Date(ts))
          val redisKey = s"DAU:$dateStr"
          if (jedis.sadd(redisKey, pageLog.mid) == 1L) {
            pageLogs.append(pageLog)
          }
        }
        jedis.close()
        println("第三方审查后：" + pageLogs.size)
        pageLogs.iterator
      }
    )

    // 维度关联
    val dauInfoDStream = redisFilterDStream.mapPartitions(
      pageLogIter => {
        val jedis = MyRedisUtil.getJedisPoolFromPoll()
        val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val dauInfos = ListBuffer[DauInfo]()
        if (pageLogIter == null) {
          println("=========================")
        }
        for (pageLog <- pageLogIter) {
          val dauInfo = new DauInfo()
          // 将pageLog的字段拷贝到dauInfo
          MyBeanUtil.copyProperties(pageLog, dauInfo)

          // 补充维度信息
          println("provinceID=" + dauInfo.province_id)
          println("dauInfo=" + dauInfo.toString)
          val userInfoStr = jedis.get(s"DIM:USER_INFO:${pageLog.user_id}")
          val userInfoJson = JSON.parseObject(userInfoStr)
          // 性别
          val gender = userInfoJson.getString("gender")
          // 年龄
          val birthday = LocalDate.parse(userInfoJson.getString("birthday"))
          val now = LocalDate.now
          val age = Period.between(birthday, now).getYears
          // 补充字段
          dauInfo.user_gender = gender
          dauInfo.user_age = age.toString
          println("===============================")
          // 地区维度
          val provinceId = dauInfo.province_id
          val redisProvinceKey = s"DIM:BASE_PROVINCE:$provinceId"
          val provinceStr = jedis.get(redisProvinceKey)
          val provinceJson = JSON.parseObject(provinceStr)
          println("redisProvinceKey=" + redisProvinceKey)
          val provinceName = provinceJson.getString("name")
          val provinceIsoCode = provinceJson.getString("iso_code")
          val provinceIso_3166_2 = provinceJson.getString("iso_3166_2")
          val provinceAreaCode = provinceJson.getString("area_code")
          // 补充字段
          dauInfo.province_name = provinceName
          dauInfo.province_iso_code = provinceIsoCode
          dauInfo.province_3166_2 = provinceIso_3166_2
          dauInfo.province_area_code = provinceAreaCode

          // 日期字段
          val date = sdf.format(new Date(pageLog.ts))
          val dates = date.split(" ")
          val dt = dates(0)
          val hr = dates(1).split(":")(0)
          // 补充到字段
          dauInfo.dt = dt
          dauInfo.hr = hr

          dauInfos.append(dauInfo)
        }
        jedis.close()
        dauInfos.iterator
      }
    )
    // dauInfoDStream.print(1000)

    // 写入到OLAP
    dauInfoDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          dauInfoIter => {
            val docs: List[(String, DauInfo)] = dauInfoIter.map(dauInfo => (dauInfo.mid, dauInfo)).toList
            if (docs.size > 0) {
              val doc: (String, DauInfo) = docs.head
              val ts = doc._2.ts
              val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
              val date: String = sdf.format(new Date(ts))
              val indexName: String = s"gmall_dau_info_$date"
              MyEsUtil.bulkSave(indexName, docs)
            }
          }
        )
        // 提交offset
        MyOffsetUtil.saveOffset(topicName, groupId, offsetRanges)
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 状态还原
   *
   * 在每次启动实时任务时，进行一次状态还原。以ES为准，将所有的mid提取出来，覆盖redis中的mid。
   * */
  def revertStart(): Unit = {
    // 从es查询mid
    val date: LocalDate = LocalDate.now()
    val indexName: String = s"gmall_dau_info_$date"
    val fieldName: String = "mid"
    val mids: List[String] = MyEsUtil.search(indexName, fieldName)
    // 删除redis中记录的状态
    val jedis = MyRedisUtil.getJedisPoolFromPoll()
    val redisDauKey: String = s"DAU:$date"
    jedis.del(redisDauKey)
    // 将查询到的mid写入redis
    if (mids != null && mids.size > 0) {
      val pipeline = jedis.pipelined()
      for (mid <- mids) {
        pipeline.sadd(redisDauKey, mid)
      }
      pipeline.sync()
    }
    jedis.close()
  }
}
