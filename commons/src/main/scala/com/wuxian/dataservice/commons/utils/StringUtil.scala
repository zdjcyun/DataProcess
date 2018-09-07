package com.wuxian.dataservice.commons.utils

import java.util.Random

/**
  * @author 伍鲜
  *
  *         字符串工具类
  */
class StringUtil {

}

/**
  * @author 伍鲜
  *
  *         字符串工具类
  */
object StringUtil {
  /**
    * * 判断一个对象是否为空
    *
    * @param obj
    * @return true：为空 false：非空
    */
  def isNull(obj: Any): Boolean = obj == null

  /**
    * * 判断一个对象是否非空
    *
    * @param obj
    * @return true：非空 false：空
    */
  def isNotNull(obj: Any): Boolean = !isNull(obj)

  /**
    * * 判断一个对象是否为空
    *
    * @param obj
    * @return true：为空 false：非空
    */
  def isEmpty(obj: Any): Boolean = {
    if (obj == null) return true
    if (obj.isInstanceOf[String]) if (obj.toString.trim == "") return true
    else if (obj.isInstanceOf[List[_]]) if (obj.asInstanceOf[List[_]].size == 0) return true
    else if (obj.isInstanceOf[Map[_, _]]) if (obj.asInstanceOf[Map[_, _]].size == 0) return true
    else if (obj.isInstanceOf[Set[_]]) if (obj.asInstanceOf[Set[_]].size == 0) return true
    else if (obj.isInstanceOf[Array[Int]]) if (obj.asInstanceOf[Array[Int]].length == 0) return true
    else if (obj.isInstanceOf[Array[Long]]) if (obj.asInstanceOf[Array[Long]].length == 0) return true
    else if (obj.isInstanceOf[Array[AnyRef]]) if (obj.asInstanceOf[Array[AnyRef]].length == 0) return true
    false
  }

  /**
    * * 判断一个对象是否非空
    *
    * @param obj
    * @return true：非空 false：空
    */
  def isNotEmpty(obj: Any): Boolean = !isEmpty(obj)

  /**
    * 判断字符串是否包含在给定的字符串中
    *
    * @param str
    * @param args
    * @return
    */
  def inStringIgnoreCase(str: String, args: String*): Boolean = !args.filter(_.equalsIgnoreCase(str)).isEmpty

  def getRandomString(length: Int): String = {
    val base = "aA0bBcC1dDeE2fFgG3hHiI4jJkK5lLmM6nNoO7pPqQ8rRsS9tTuUvVwWxXyYzZ"
    val random = new Random

    (0 until length).map(x => {
      base.charAt(random.nextInt(base.length))
    }).mkString("")
  }
}