package com.service.data.spark.sql.utils

import com.service.data.spark.sql.udfs.UDFs
import org.apache.spark.sql.SparkSession

/**
  * @author 伍鲜
  *
  *         SparkSession工具
  */
object SparkSessionUtil {

  /**
    * SparkSession功能扩展
    *
    * @param spark
    */
  implicit class SparkSessionImplicit(spark: SparkSession) {
    /**
      * 注册自定义函数
      */
    def withDefaultUdfs(): SparkSession = {
      // 分布式ID生成
      spark.sqlContext.udf.register("GenerateID", UDFs.GenerateID)
      // round
      spark.sqlContext.udf.register("roundDecimal", UDFs.roundDecimal(_: java.math.BigDecimal, _: Int))
      // bround
      spark.sqlContext.udf.register("broundDecimal", UDFs.broundDecimal(_: java.math.BigDecimal, _: Int))

      spark
    }
  }

  /**
    * 获取SparkSession
    *
    * @return
    */
  def getSparkSession(): SparkSession = getSparkSession("SparkSQLApplication")

  /**
    * 获取SparkSession
    *
    * @return
    */
  def getSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/tbdss.DocTest")
      .config("spark.mongodb.output.uri", "mongodb://localhost:27017/tbdss.DocTest")
      .config("spark.redis.host", "localhost")
      .config("spark.redis.port", "6379")
      .getOrCreate()
      .withDefaultUdfs()
  }
}
