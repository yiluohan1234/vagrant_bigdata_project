package com.atguigu.spark.core.framework.application

import com.atguigu.spark.core.framework.common.TApplication
import com.atguigu.spark.core.framework.controller.HotCategoryTop10AnalysisController

object HotCategoryTop10AnalysisApplication extends App with TApplication{

  start() {
    val controller = new HotCategoryTop10AnalysisController()
    controller.dispatch()
  }

}
