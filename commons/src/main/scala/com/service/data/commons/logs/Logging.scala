package com.service.data.commons.logs

import org.slf4j.{Logger, LoggerFactory}

/**
  * @author 伍鲜
  *
  *         日志处理
  */
trait Logging {

  private var logger: Logger = null

  private def logName = {
    this.getClass.getName.stripSuffix("$")
  }

  protected def log: Logger = {
    if (logger == null) {
      logger = LoggerFactory.getLogger(logName)
    }
    logger
  }

  def trace(msg: => String): Unit = if (log.isTraceEnabled) log.trace(msg)

  def trace(msg: => String, err: Throwable): Unit = if (log.isTraceEnabled) log.trace(msg, err)

  def debug(msg: => String): Unit = if (log.isDebugEnabled) log.debug(msg)

  def debug(msg: => String, err: Throwable): Unit = if (log.isDebugEnabled) log.debug(msg, err)

  def info(msg: => String): Unit = if (log.isInfoEnabled) log.info(msg)

  def info(msg: => String, err: Throwable): Unit = if (log.isInfoEnabled) log.info(msg, err)

  def warn(msg: => String): Unit = if (log.isWarnEnabled) log.warn(msg)

  def warn(msg: => String, err: Throwable): Unit = if (log.isWarnEnabled) log.warn(msg, err)

  def error(msg: String): Unit = log.error(msg)

  def error(msg: String, err: Throwable): Unit = log.error(msg, err)
}