package com.service.data.spark.sql.paging

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document
import org.bson.types.ObjectId

import scala.beans.BeanProperty
import scala.collection.mutable.ArrayBuffer

/**
  * @author 伍鲜
  *
  *         MongoSpark分页加载MongoDB的数据
  *         不支持自定义排序，因为：
  *         1、如果数据量过大，需要给排序字段创建索引；
  *         2、自定义排序不利于分页边界的确定。
  */
class SparkMongoPaging private() {
  /**
    * 分页大小，默认10000
    */
  @BeanProperty var pageSize: Int = 10000
  /**
    * MongoSpark ReadConfig
    */
  @BeanProperty var readConfig: ReadConfig = _

  /**
    * 查询条件
    *
    * 例如：
    * 1、根据某个字段值查询：
    *    Document.parse("{key : value}")
    * 或者
    * new Document(key, value)
    * 2、根据多个字段值查询：
    *    Document.parse("{key1 : value1, key2 : value2, ..., keyn : valuen}")
    * 或者
    * new Document(key1, value1).append(key2, value2)....append(keyn, valuen)
    * 3、同时满足多个条件：
    *    Document.parse("{ $and : [{key1 : value1},{key2 : value2}]}")
    * 或者
    *    Document.parse("{ $or : [{key1 : value1},{key2 : value2}]}")
    * 或者
    *    Document.parse("{ $nor : [{key1 : value1},{key2 : value2}]}")
    */
  @BeanProperty var queryDocument: Document = new Document()

  /**
    * 筛选字段：仅返回需要的字段
    */
  @BeanProperty var projectFields: Seq[String] = Seq()

  /**
    * 当前分页的objectId下限（不包含）
    */
  private var minimumObjectId: String = _

  /**
    * SparkContext
    */
  private var sparkContext: SparkContext = _

  /**
    * 空Document
    */
  private val emptyDocument: Document = new Document()

  def this(spark: SparkSession) {
    this()
    this.sparkContext = spark.sparkContext
    this.readConfig = ReadConfig(spark)
  }

  def this(context: SparkContext) {
    this()
    this.sparkContext = context
    this.readConfig = ReadConfig(context)
  }

  /**
    * 构建查询条件
    *
    * @param minimumObjectId
    * @return
    */
  private def buildQueryBson(minimumObjectId: String): Document = {
    val queryBuffer = new ArrayBuffer[Document]()
    if (minimumObjectId != null) {
      queryBuffer.append(Document.parse("""{ "_id" : { $gt : ObjectId("""" + minimumObjectId + """")}}"""))
    }
    if (queryDocument != null && queryDocument.size() > 0) {
      queryBuffer.append(queryDocument)
    }

    if (queryBuffer.size > 0) {
      Document.parse("""{ $match : { $and : [""" + queryBuffer.map(_.toJson()).mkString(",") + """] } }""")
    } else {
      emptyDocument
    }
  }

  /**
    * 判断是否还有下一页数据
    *
    * @return
    */
  def hasNextPage(): Boolean = {
    val pipelineBuffer = new ArrayBuffer[Document]()
    // 根据查询条件
    val queryBson = buildQueryBson(minimumObjectId)
    if (queryBson.size() > 0) {
      pipelineBuffer.append(queryBson)
    }
    // 仅查询ID字段
    pipelineBuffer.append(Document.parse("""{ $project : { "_id" : 1 } }"""))
    // ID字段升序排序
    pipelineBuffer.append(Document.parse("""{ $sort : { "_id" : 1 } }"""))
    // 仅返回一条数据
    pipelineBuffer.append(Document.parse("""{ $limit : 1 }"""))
    // 仅判断有无
    MongoSpark.load(sparkContext, readConfig).withPipeline(pipelineBuffer).count() > 0
  }

  /**
    * 获取下一页数据
    *
    * @return
    */
  def nextPage(): DataFrame = {
    val pipelineBuffer = new ArrayBuffer[Document]()
    // 根据查询条件
    val queryBson = buildQueryBson(minimumObjectId)
    if (queryBson.size() > 0) {
      pipelineBuffer.append(queryBson)
    }
    // 仅查询指定字段
    if (projectFields != null && projectFields.size > 0) {
      pipelineBuffer.append(Document.parse("{ $project : { " + projectFields.map(x => s"${x} : 1").mkString(",") + " }}"))
    }
    // ID字段升序排序
    pipelineBuffer.append(Document.parse("""{ $sort : { "_id" : 1 } }"""))
    // 仅返回指定条数
    pipelineBuffer.append(Document.parse("{ $limit : " + pageSize + " }"))

    // 根据条件加载数据
    val rdd = MongoSpark.load(sparkContext, readConfig).withPipeline(pipelineBuffer)

    // 记录一下当前分页的ID上界，也就是下一个分页的下界
    minimumObjectId = rdd.map(_.get("_id").asInstanceOf[ObjectId].toHexString).max()

    // 返回当前分页的数据
    rdd.toDF()
  }
}
