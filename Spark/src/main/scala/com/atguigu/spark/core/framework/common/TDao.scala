package com.atguigu.spark.core.framework.common

import com.atguigu.spark.core.framework.utils.EnvUtil

trait TDao {

  def readFile(path:String) = {
    EnvUtil.take().textFile(path)
  }
}