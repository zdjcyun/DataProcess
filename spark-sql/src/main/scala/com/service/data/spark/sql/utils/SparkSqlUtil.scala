package com.service.data.spark.sql.utils

import org.apache.spark.sql.SparkSession

object SparkSqlUtil {
  def getSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("SparkSQLApplication")
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/tbdss.DocTest")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/tbdss.DocTest")
      .config("spark.redis.host", "localhost")
      .config("spark.redis.port", "6379")
      .getOrCreate()
  }
}
