package com.service.data.spark.sql.utils

import com.service.data.spark.sql.udfs.UDFs
import org.apache.spark.sql.SparkSession

object SparkSqlUtil {
  def getSparkSession(): SparkSession = {
    val builder = SparkSession.builder()
      .appName("SparkSQLApplication")
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/tbdss.DocTest")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/tbdss.DocTest")
      .config("spark.redis.host", "localhost")
      .config("spark.redis.port", "6379")

    val spark = builder.getOrCreate()

    spark.sqlContext.udf.register("broundDecimal", UDFs.broundDecimal(_: java.math.BigDecimal, _: Int))

    spark
  }
}
