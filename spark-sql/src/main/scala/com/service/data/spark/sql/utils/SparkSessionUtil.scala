package com.service.data.spark.sql.utils

import com.service.data.commons.property.ServiceProperty
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
    val builder = SparkSession.builder()
      .appName(appName)
      .master("local[*]")

    // MongoDB 参数设置
    ServiceProperty.properties.filter(_._1.startsWith("spark.mongodb")).foreach(x => {
      builder.config(x._1, x._2)
    })

    // Redis 参数设置
    ServiceProperty.properties.filter(_._1.startsWith("spark.redis")).foreach(x => {
      builder.config(x._1, x._2)
    })

    val spark = builder.getOrCreate()

    spark.withDefaultUdfs()

    spark
  }
}
