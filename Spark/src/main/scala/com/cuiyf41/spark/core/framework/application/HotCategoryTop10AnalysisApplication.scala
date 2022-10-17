package com.cuiyf41.spark.core.framework.application

import com.cuiyf41.spark.core.framework.common.TApplication
import com.cuiyf41.spark.core.framework.controller.HotCategoryTop10AnalysisController

object HotCategoryTop10AnalysisApplication extends App with TApplication{

  start() {
    val controller = new HotCategoryTop10AnalysisController()
    controller.dispatch()
  }

}
