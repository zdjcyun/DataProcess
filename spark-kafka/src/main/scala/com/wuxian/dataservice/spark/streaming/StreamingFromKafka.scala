package com.wuxian.dataservice.spark.streaming

import java.text.SimpleDateFormat

import com.wuxian.dataservice.commons.PubFunction
import com.wuxian.dataservice.commons.dbs.DBConn
import com.wuxian.dataservice.commons.utils.StringUtil
import com.wuxian.dataservice.spark.streaming.process.TopicValueProcess
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkEnv}
import scalikejdbc._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * @author 伍鲜
  *
  *         Spark Streaming 消费 Kafka 的数据
  */
class StreamingFromKafka {

}

/**
  * @author 伍鲜
  *
  *         Spark Streaming 消费 Kafka 的数据
  */
object StreamingFromKafka extends PubFunction {
  def main(args: Array[String]): Unit = {
    /**
      * 参数配置信息。
      */
    var paramConfig: Map[String, String] = Map.empty[String, String]
    /**
      * 主题对应的表。(主题， 表名， 字段分隔符)
      */
    var topicTables: Array[(String, String, String)] = Array.empty[(String, String, String)]

    /**
      * 表包含的字段。（表名， 字段名称， 字段类型， 字段顺序， 字段值）
      */
    var tableColumns: Array[(String, String, String, Int, String)] = Array.empty[(String, String, String, Int, String)]

    /**
      * 主题的码值转换。（主题， 字段名称， 源代码值， 目标代码值）
      */
    var topicCodeMapping: Array[(String, String, String, String)] = Array.empty[(String, String, String, String)]

    DBConn.setupAll()
    NamedDB('config) readOnly ({ implicit session =>
      paramConfig = sql"select param_name,param_value from bfb_t_param_config_list".map(rs => Map(rs.string(1) -> rs.string(2))).list().apply().reduce(_ ++ _)
      topicTables = sql"select topic_name,table_name,field_split from bfb_t_topic_table_list".map(rs => (rs.string(1), rs.string(2), rs.string(3))).list().apply().toArray
      tableColumns = sql"select table_name,column_name,column_type,column_index,column_data from bfb_t_table_column_list".map(rs => (rs.string(1), rs.string(2), rs.string(3), rs.int(4), rs.string(5))).list().apply().toArray
      topicCodeMapping = sql"select topic_name,column_name,source_code,target_code from bfb_t_topic_code_mapping".map(rs => (rs.string(1), rs.string(2), rs.string(3), rs.string(4))).list().apply().toArray
    })

    val tableMap = topicTables.map(table => Map(table._1 -> table)).reduce(_ ++ _)
    val columnMap = tableColumns.groupBy(_._1).map(column => (column._1 -> column._2.sortWith(_._4 < _._4).map(x => (x._2, x._3, x._4, x._5))))
    val mappingMap = topicCodeMapping.groupBy(_._1).map(mapping => (mapping._1, mapping._2.map(x => (x._2, x._3, x._4))))

    val sparkConf = new SparkConf()
      .setAppName(paramConfig.get("spark.streaming.application.name").getOrElse("SparkStreamingKafkaToDatabase"))
      // 部署打包的时候需要去掉下面这行
      .setMaster("yarn-client").set("yarn.resourcemanager.hostname", "").set("spark.executor.instances", "2").setJars(Seq())

    val ssc = new StreamingContext(sparkConf, Seconds(paramConfig.get("spark.streaming.batch.duration").getOrElse("5").toLong))

    if (!"nocp".equalsIgnoreCase(paramConfig.get("spark.streaming.checkpoint.dir").getOrElse("nocp"))) {
      ssc.checkpoint(paramConfig.get("spark.streaming.checkpoint.dir").get)
    }

    val kafkaParams = Map[String, String](
      "metadata.boker.list" -> paramConfig("kafka.bootstrap.servers")
    )

    val topics = paramConfig("kafka.consumer.topics").split(",", -1)

    topics.foreach(topic => {
      val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic)).map(_._2)
      lines.foreachRDD(rdd => {
        // 无数据，不执行
        if (!rdd.isEmpty()) {
          // Partition的数据在Executor上运行
          rdd.foreachPartition(data => {
            // 根据主题取表
            val table = tableMap(topic)
            // 根据表名取字段
            val columns = columnMap(table._2)
            Future {
              process(StreamingMessage(table, columns, data, mappingMap.get(topic).getOrElse(Array()), paramConfig.get(s"spark.${topic}.process.class").getOrElse("com.wuxian.dataservice.spark.streaming.process.DefaultTopicProcess")))
            }
          })
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 数据类型转换
    *
    * @param column 字段配置
    * @return
    */
  private def convert(column: ((String, String, Int, String), String)): Any = {
    if (StringUtil.isEmpty(column._2)) {
      ""
    } else {
      try {
        column._1._2.toLowerCase.trim match {
          case "int" => column._2.toInt
          case "long" => column._2.toLong
          case "double" => column._2.toDouble

          case "date" => new SimpleDateFormat(column._1._4).parse(column._2)
          case "datetime" => new SimpleDateFormat(column._1._4).parse(column._2)
          case "timestamp" => new SimpleDateFormat(column._1._4).parse(column._2)

          case _ => column._2
        }
      } catch {
        case ex: Exception =>
          println(s"${SparkEnv.get.executorId}:${Thread.currentThread.getId}-${Thread.currentThread.getName}:${ex.getMessage}")
          null
      }
    }
  }

  /**
    * 处理消费到的数据
    *
    * @param message 消费消息
    */
  private def process(message: StreamingMessage): Unit = {
    try {
      val dataList = message.rdd
        // 得到数据解析后的字段值
        .map(x => TopicValueProcess.getProcess(message.className).convertToColumns(x, message.table._3))
        // 过滤掉字段个数不满足要求的数据
        .filter(_.length == message.columns.length)
        // 将字段与字段值整合
        .map(x => message.columns.zip(x))
        // 对字段值进行码值转换
        .map(x => x.map(column => (column._1, message.mapping.filter(_._1 == column._1._1).filter(_._2 == column._2).map(_._3).headOption.getOrElse(column._2))))
        // 数据类型转换
        .map(x => x.map(convert(_)))
        // 转换成序列
        .map(_.toSeq)
        // 序列
        .toSeq

      val insert = s"insert into ${message.table._2} (${message.columns.map(_._1).mkString(",")})"
      val values = s"values (${message.columns.map(x => "?").mkString(",")})"

      try {
        // 验证数据源是否初始化
        NamedDB('config) localTx { implicit session =>
          sql"select count(*) from bfb_t_param_config_list".map(rs => rs.int(1)).single().apply()
        }
      } catch {
        case ex: Exception =>
          println(s"${SparkEnv.get.executorId}:${Thread.currentThread.getId}-${Thread.currentThread.getName}:${ex.getMessage}")
          DBConn.setupAll()
      }
      try {
        // 将数据保存到云环境
        NamedDB('mysql) localTx { implicit session =>
          SQL(s"${insert}${values}").batch(dataList: _*).apply()
        }
      } catch {
        case ex: Exception =>
          println(s"${SparkEnv.get.executorId}:${Thread.currentThread.getId}-${Thread.currentThread.getName}:${ex.getMessage}")
      }
      try {
        // 将数据保存到集市
        NamedDB('oracle) localTx { implicit session =>
          SQL(s"${insert}${values}").batch(dataList: _*).apply()
        }
      } catch {
        case ex: Exception =>
          println(s"${SparkEnv.get.executorId}:${Thread.currentThread.getId}-${Thread.currentThread.getName}:${ex.getMessage}")
      }
    } catch {
      case ex: Exception =>
        println(s"${SparkEnv.get.executorId}:${Thread.currentThread.getId}-${Thread.currentThread.getName}:${ex.getMessage}")
    }
  }
}
