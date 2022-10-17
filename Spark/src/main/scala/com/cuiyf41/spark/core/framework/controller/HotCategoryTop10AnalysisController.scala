package com.cuiyf41.spark.core.framework.controller

import com.cuiyf41.spark.core.framework.common.TController
import com.cuiyf41.spark.core.framework.service.HotCategoryTop10AnalysisService

class HotCategoryTop10AnalysisController extends TController {
  private val service = new HotCategoryTop10AnalysisService()

  def dispatch(): Unit ={
    // TODO 执行业务操作
    val array = service.dataAnalysis()
    array.foreach(println)

  }

}
