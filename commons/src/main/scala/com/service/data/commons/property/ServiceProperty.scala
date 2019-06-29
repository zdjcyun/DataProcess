package com.service.data.commons.property

/**
  * @author 伍鲜
  *
  *         应用程序参数配置
  */
object ServiceProperty {
  lazy val properties = SparkPropertyHandlerFactory.getSparkPropertyHandler().getProperties
}
