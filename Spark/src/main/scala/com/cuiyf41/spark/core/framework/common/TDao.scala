package com.cuiyf41.spark.core.framework.common

import com.cuiyf41.spark.core.framework.utils.EnvUtil

trait TDao {

  def readFile(path:String) = {
    EnvUtil.take().textFile(path)
  }
}