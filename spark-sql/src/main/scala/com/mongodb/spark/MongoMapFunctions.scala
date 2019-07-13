package com.mongodb.spark

import com.mongodb.spark.sql.MapFunctions
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.bson.BsonDocument

/**
  * @author 伍鲜
  *
  *         MapFunctions工具
  *         单独将该类提取到com.mongodb.spark包下的原因是：
  *         MongoSpark限制了方法的权限范围：private[spark] def rowToDocumentMapper(schema: StructType, extendedBsonTypes: Boolean): (Row) => BsonDocument
  */
object MongoMapFunctions {
  /**
    * 将Row转换成BsonDocument
    *
    * @param schema 模式
    * @param extendedBsonTypes
    * @return
    */
  def rowToDocumentMapper(schema: StructType, extendedBsonTypes: Boolean): (Row) => BsonDocument = MapFunctions.rowToDocumentMapper(schema, extendedBsonTypes)
}
