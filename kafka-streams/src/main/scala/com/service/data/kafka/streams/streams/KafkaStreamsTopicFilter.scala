package com.service.data.kafka.streams.streams

import java.util.Properties

import com.service.data.commons.PubFunction
import com.service.data.commons.dbs.DBConn
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{KStreamBuilder, Predicate}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import scalikejdbc._
import scalikejdbc.config.DBs

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

  /**
    * 参数配置信息。
    */
  var paramConfig: Map[String, String] = Map.empty[String, String]

  val symbol = 'config
  DBConn.setup(symbol)
  NamedDB(symbol) readOnly ({ implicit session =>
    paramConfig = sql"select param_name,param_value from bfb_t_param_config_list".map(rs => Map(rs.string(1) -> rs.string(2))).list().apply().reduce(_ ++ _)
  })
  DBs.close(symbol)

  val props = new Properties()

  props.put(StreamsConfig.APPLICATION_ID_CONFIG, paramConfig.get("kafka.application.id").getOrElse("KafkaStreamsTopicFilter"))
  props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, paramConfig("kafka.bootstrap.servers"))

  props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
  props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)

  val builder: KStreamBuilder = new KStreamBuilder

  paramConfig.keySet.toArray.filter(k => !"kafka.(.*).source.topic".r.findAllIn(k.toString).isEmpty).map(k => k.toString.split(RegexpEscape("."), -1)(1)).distinct.foreach(group => {
    builder.stream(paramConfig.get(s"kafka.${group}.source.topic").get).filter(new Predicate[String, String] {
      override def test(k: String, v: String): Boolean = {
        !paramConfig.get(s"kafka.${group}.regex.string").get.r.findAllIn(v).isEmpty
      }
    }).to(paramConfig.get(s"kafka.${group}.target.topic").get)
  })

  val streams = new KafkaStreams(builder, props)
  streams.start()
}
