package org.apache.spark.sql

object SparkDatasetUtil {
  def debugShow(ds: Dataset[_], numRows: Int, truncate: Boolean): String = {
    if (truncate) {
      ds.showString(numRows, 20)
    } else {
      ds.showString(numRows, 0)
    }
  }
}
