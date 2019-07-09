package com.service.data.kafka.clients.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.JavaConversions._

/**
  * @author 伍鲜
  *
  *         简单的Kafka生产者
  */
object SimpleProducer {
  /**
    * Kafka生产者相关参数
    */
  private val kafkaProducerProperties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "service.hy-wux.com:9092")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "SimpleProducerExample")
    props.put(ProducerConfig.ACKS_CONFIG, "1")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    props
  }

  /**
    * Kafka生产者创建
    */
  val producer = {
    val producer = new KafkaProducer[String, String](kafkaProducerProperties)
    sys.addShutdownHook({
      producer.close()
    })
    producer
  }

  def apply[K, V](): KafkaProducer[K, V] = apply(kafkaProducerProperties)

  def apply[K, V](config: Properties): KafkaProducer[K, V] = apply(config.toMap)

  def apply[K, V](config: Map[String, Object]): KafkaProducer[K, V] = {
    val producer = new KafkaProducer[K, V](config)
    sys.addShutdownHook({
      producer.close()
    })
    producer
  }
}
