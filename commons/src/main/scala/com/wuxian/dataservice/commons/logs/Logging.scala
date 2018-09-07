package com.wuxian.dataservice.commons.logs

import org.apache.log4j.Logger

/**
  * @author 伍鲜
  *
  *         日志处理
  */
trait Logging {

  private var log_ : Logger = null

  private def logName = {
    this.getClass.getName.stripSuffix("$")
  }

  protected val deepLogs = (System.getProperties != null && System.getProperties.containsKey("dl")) match {
    case true => System.getProperty("dl").equalsIgnoreCase("dl")
    case false => false
  }

  protected def log: Logger = {
    if (log_ == null) {
      log_ = Logger.getLogger(logName)
    }
    log_
  }
}