package com.service.data.spark.sql.utils

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @author 伍鲜
  *
  *         Redis工具类
  */
object RedisUtil {

  /**
    * 加载Redis中的数据。加载Redis中key为："${tableName}:${keyColumn.value}"的所有数据形成一个DataFrame
    *
    * @param tableName 表名称，即Redis的key前缀
    * @param keyColumn 主键字段，即Redis的key后缀
    * @param spark
    * @return
    */
  def loadFromRedis(tableName: String, keyColumn: String)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .format("org.apache.spark.sql.redis")
      .option("table", tableName)
      .option("key.column", keyColumn)
      .load()
  }

  /**
    * 写入数据到Redis。将DataFrame中的每一行数据转成Hash存放到Redis，Redis的key为："${tableName}:df(keyColumn)"，类型为Hash
    *
    * @param df        需要写入的数据
    * @param tableName 表名称，即Redis的key前缀
    * @param keyColumn 主键字段，即Redis的key后缀
    * @param mode      写入方式
    * @param spark
    */
  def writeToRedis(df: DataFrame, tableName: String, keyColumn: String, mode: SaveMode)(implicit spark: SparkSession): Unit = {
    df.write
      .format("org.apache.spark.sql.redis")
      .option("table", tableName)
      .option("key.column", keyColumn)
      .mode(mode)
      .save()
  }
}