package com.service.data.examples.spark.sql.mongodb

import com.service.data.spark.sql.implicits.SparkSqlImplicit._
import com.service.data.spark.sql.paging.SparkMongoPaging
import com.service.data.spark.sql.utils.{MongoUtil, SparkSqlUtil}
import org.bson.Document

/**
  * @author 伍鲜
  *
  *         分页读取MongoDB形成Dataset/DataFrame
  */
object SparkSqlMongoReadPage {
  def main(args: Array[String]): Unit = {
    implicit val spark = SparkSqlUtil.getSparkSession()

    val paging = new SparkMongoPaging(spark)

    paging.pageSize = 1000

    paging.queryDocument = Document.parse("""{ $nor : [{ name : "伍鲜1707520481" }]}""")
    // paging.queryDocument = Document.parse("""{ $or : [{ name : "伍鲜1707520481" }, { name:"伍鲜632003428"}]}""")
    paging.projectFields = Seq("name", "age")

    while (paging.hasNextPage()) {
      // 读取MongoDB
      val df = paging.nextPage()
      df.printSchema()
      df.debugShow()
    }
  }
}
