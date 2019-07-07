package com.service.data.examples.spark.sql

import com.service.data.spark.sql.utils.{RedisUtil, SparkSqlUtil}
import com.service.data.spark.sql.implicits.SparkSqlImplicit._

/**
  * @author 伍鲜
  *
  *         读取Redis形成Dataset/DataFrame
  */
object SparkSqlRedisRead {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSqlUtil.getSparkSession()

    // 读取到Redis
    val df = RedisUtil.loadFromRedis("users", "name")
    df.printSchema()
    df.debugShow()
  }
}
