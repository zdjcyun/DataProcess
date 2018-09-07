package com.wuxian.dataservice.spark.streaming.process

/**
  * @author 伍鲜
  *
  *         默认的Topic数据处理实现类，处理按字段顺序用字段分隔符进行分隔的Topic数据。
  */
class DefaultTopicProcess extends TopicValueProcess {
  /**
    * 将Topic的值转换成所需要的字段值数组
    *
    * @param value Topic 的值
    * @param split 字段分隔符
    * @return 根据Topic的值转换得到的字段值数组
    */
  override def convertToColumns(value: String, split: String): Array[String] = {
    value.split(RegexpEscape(Hex2String(split)), -1)
  }
}
