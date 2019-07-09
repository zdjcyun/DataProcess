package com.service.data.examples.spark.sql.mongodb

import com.service.data.kafka.clients.producer.SimpleProducer
import com.service.data.spark.sql.paging.SparkMongoPaging
import com.service.data.spark.sql.utils.SparkSqlUtil
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.{SparkEnv, TaskContext}
import org.bson.Document

/**
  * @author 伍鲜
  *
  *         分页读取MongoDB的数据并写入到Kafka
  */
object SparkSqlMongoReadToKafka {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSqlUtil.getSparkSession()

    val paging = new SparkMongoPaging(spark)

    paging.pageSize = 10000

    paging.queryDocument = Document.parse("""{ $nor : [{ name : "伍鲜1707520481" }]}""")
    // paging.queryDocument = Document.parse("""{ $or : [{ name : "伍鲜1707520481" }, { name:"伍鲜632003428"}]}""")
    paging.projectFields = Seq("name", "age")

    while (paging.hasNextPage()) {
      val ds = paging.nextPage().toJSON
      ds.foreachPartition(rdd => {
        rdd.foreach(record => {
          SimpleProducer.producer.send(new ProducerRecord[String, String]("ReadFromMongo", record))
          println(s"${SparkEnv.get.executorId} =====> ${TaskContext.get().partitionId()} =====> ${SimpleProducer.producer} =====> ${record}")
        })
      })
    }
  }
}
