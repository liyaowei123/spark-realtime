package edu.lyw.utils

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util
import scala.collection.mutable

/**
*kafka工具类，用于生产和消费
**/
object MyKafkaUtil {
  /**
   * kafka生产者对象
   * */
  private val producer: KafkaProducer[String, String] = createProducer()
  /**
   *消费者配置
   * */
  private val consumerConfigs: mutable.Map[String, Object] = mutable.Map[String, Object](
    // kafka集群位置
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropertiesUtil(MyConfig.KAFKA_BOOTSTRAP_SERVER),
    // kv反序列化器
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    // offset提交
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG ->  "true",
    // offset定位到latest
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
  )
  /**
   * 基于spark streaming消费，获取到kafkaDstream
   * */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String) = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs))
    kafkaDStream
  }
  /**
   * 基于spark streaming消费，指定offset
   * */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String, offset: Map[TopicPartition, Long]) = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs, offset))
    kafkaDStream
  }
  /**
   * 创建kafka生产者对象
   * */
  def createProducer(): KafkaProducer[String, String] = {
    // 生产者配置类
    val producerConfig = new util.HashMap[String, AnyRef]
    // kafka集群位置
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MyPropertiesUtil(MyConfig.KAFKA_BOOTSTRAP_SERVER))
    // kv序列化器
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    // acks
    producerConfig.put(ProducerConfig.ACKS_CONFIG, "all")
    // 幂等配置
    producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    // 创建kafka生产者
    val _producer = new KafkaProducer[String, String](producerConfig)
    _producer
  }
  /**
   * 生产数据（按照默认的粘性分区）
   * */
  def send(topic:String, msg: String) = {
    producer.send(new ProducerRecord[String, String](topic, msg))
  }
  /**
   * 生产数据（按照指定key分区）
   * */
  def send(topic: String, msg: String, key:String) = {
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }
  /**
   * 关闭生产者对象
   * */
  def close = if (producer != null) {producer.close()}
  /**
   * 把数据从缓冲区刷新到broker
   * */
  def flush = producer.flush()
}
