package com.service.data.spark.sql.udfs

/**
  * 自定义函数
  */
object UDFs {
  /**
    * Decimal类型的数据保留指定精度。Spark SQL中的bround函数，只支持所有行采用统一精度，不支持不同行采用不同精度。
    *
    * @param value
    * @param scale
    * @return
    */
  def broundDecimal(value: java.math.BigDecimal, scale: Int): java.math.BigDecimal = {
    value.setScale(scale, java.math.BigDecimal.ROUND_HALF_EVEN)
  }
}
