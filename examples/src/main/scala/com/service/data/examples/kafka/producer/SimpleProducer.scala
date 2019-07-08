package com.service.data.examples.kafka.producer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer

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
    props.put(ProducerConfig.ACKS_CONFIG, 1)
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

}
