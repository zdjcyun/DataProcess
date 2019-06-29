package com.service.data.commons.utils

/**
  * @author 伍鲜
  *
  *         通用工具类
  */
class CommUtil {

}

/**
  * @author 伍鲜
  *
  *         通用工具类
  */
object CommUtil {

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
    else if (obj.isInstanceOf[String] && obj.toString.trim == "") return true
    else if (obj.isInstanceOf[List[_]] && obj.asInstanceOf[List[_]].size == 0) return true
    else if (obj.isInstanceOf[Map[_, _]] && obj.asInstanceOf[Map[_, _]].size == 0) return true
    else if (obj.isInstanceOf[Set[_]] && obj.asInstanceOf[Set[_]].size == 0) return true
    else if (obj.isInstanceOf[Array[Int]] && obj.asInstanceOf[Array[Int]].length == 0) return true
    else if (obj.isInstanceOf[Array[Long]] && obj.asInstanceOf[Array[Long]].length == 0) return true
    else if (obj.isInstanceOf[Array[AnyRef]] && obj.asInstanceOf[Array[AnyRef]].length == 0) return true
    else false
  }

  /**
    * * 判断一个对象是否非空
    *
    * @param obj
    * @return true：非空 false：空
    */
  def isNotEmpty(obj: Any): Boolean = !isEmpty(obj)

}