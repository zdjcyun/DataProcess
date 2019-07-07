package com.service.data.examples.spark.sql.mongodb

import com.service.data.spark.sql.implicits.SparkSqlImplicit._
import com.service.data.spark.sql.utils.{MongoUtil, SparkSqlUtil}

/**
  * @author 伍鲜
  *
  *         读取MongoDB形成Dataset/DataFrame
  */
object SparkSqlMongoRead {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSqlUtil.getSparkSession()

    // 读取MongoDB
    val df = MongoUtil.loadFromMongoDB("DocTest")
    df.printSchema()
    df.debugShow()
  }
}
