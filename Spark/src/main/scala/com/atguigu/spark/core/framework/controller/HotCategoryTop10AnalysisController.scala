package com.atguigu.spark.core.framework.controller

import com.atguigu.spark.core.framework.common.TController
import com.atguigu.spark.core.framework.service.HotCategoryTop10AnalysisService

class HotCategoryTop10AnalysisController extends TController {
  private val service = new HotCategoryTop10AnalysisService()

  def dispatch(): Unit ={
    // TODO 执行业务操作
    val array = service.dataAnalysis()
    array.foreach(println)

  }

}
