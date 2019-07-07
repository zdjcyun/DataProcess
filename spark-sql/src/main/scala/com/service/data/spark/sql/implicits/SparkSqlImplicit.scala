package com.service.data.spark.sql.implicits

import com.service.data.commons.logs.Logging
import org.apache.spark.sql.{Dataset, SparkDatasetUtil}

import scala.util.Try

object SparkSqlImplicit {

  /**
    * 扩展Dataset的方法
    *
    * @param ds
    */
  implicit class DatasetUtil(val ds: Dataset[_]) extends Logging {
    /**
      * 判断是否包含指定的列
      *
      * @param columnName 列名称
      * @return
      */
    def hasColumn(columnName: String): Boolean = {
      Try(ds(columnName)).isSuccess
    }

    /**
      * 判断是否包含指定的全部列
      *
      * @param columnNames 列名称
      * @return
      */
    def hasAllColumns(columnNames: Seq[String]): Boolean = {
      columnNames.map(columnName => Try(ds(columnName)).isSuccess).reduce(_ && _)
    }

    def debugShow(): Unit = debugShow(20)

    def debugShow(numRows: Int): Unit = debugShow(numRows, false)

    def debugShow(numRows: Int, truncate: Boolean): Unit = {
      debug("数据集的内容为：\n" + SparkDatasetUtil.debugShow(ds, numRows, truncate))
    }
  }

}
