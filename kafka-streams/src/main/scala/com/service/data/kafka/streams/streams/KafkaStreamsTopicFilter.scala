package com.service.data.kafka.streams.streams

import java.util.Properties

import com.service.data.commons.PubFunction
import com.service.data.commons.property.ServiceProperty
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{KStreamBuilder, Predicate}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

/**
  * @author 伍鲜
  *
  *         Kafka Topic 的数据过滤
  */
class KafkaStreamsTopicFilter {

}

/**
  * @author 伍鲜
  *
  *         Kafka Topic 的数据过滤
  */
object KafkaStreamsTopicFilter extends App with PubFunction {

  val props = new Properties()

  props.put(StreamsConfig.APPLICATION_ID_CONFIG, ServiceProperty.properties.get("kafka.streams.application.id").getOrElse("KafkaStreamsTopicFilter"))
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ServiceProperty.properties.get("kafka.bootstrap.servers").get)

  props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)

  val builder: KStreamBuilder = new KStreamBuilder

  ServiceProperty.properties.keySet.toArray.filter(k => !"kafka.streams.(.*).source.topic".r.findAllIn(k.toString).isEmpty).map(k => k.toString.split(RegexpEscape("."), -1)(1)).distinct.foreach(group => {
    builder.stream(ServiceProperty.properties.get(s"kafka.streams.${group}.source.topic").get).filter(new Predicate[String, String] {
      override def test(k: String, v: String): Boolean = {
        !ServiceProperty.properties.get(s"kafka.streams.${group}.regex.string").get.r.findAllIn(v).isEmpty
      }
    }).to(ServiceProperty.properties.get(s"kafka.streams.${group}.target.topic").get)
  })

  val streams = new KafkaStreams(builder, props)
  streams.start()
}
