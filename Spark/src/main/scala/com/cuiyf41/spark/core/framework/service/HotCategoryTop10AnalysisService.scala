package com.cuiyf41.spark.core.framework.service

import com.cuiyf41.spark.core.framework.bean.{HotCategory, HotCategoryAccumulator}
import com.cuiyf41.spark.core.framework.common.TService
import com.cuiyf41.spark.core.framework.dao.HotCategoryTop10AnalysisDao
import com.cuiyf41.spark.core.framework.utils.EnvUtil

import scala.collection.mutable

class HotCategoryTop10AnalysisService extends TService {

  private val dao = new HotCategoryTop10AnalysisDao()

  def dataAnalysis(): List[HotCategory] = {
    // 1. 读取原始日志数据
    val actionRDD = dao.readFile("datas/core/user_visit_action.txt")

    val acc = new HotCategoryAccumulator
    EnvUtil.take().register(acc, "hotCategory")

    // 2. 将数据转换结构
    actionRDD.foreach(
      action => {
        val datas = action.split("_")
        if (datas(6) != "-1") {
          // 点击的场合
          acc.add((datas(6), "click"))
        } else if (datas(8) != "null") {
          // 下单的场合
          val ids = datas(8).split(",")
          ids.foreach(
            id => {
              acc.add( (id, "order") )
            }
          )
        } else if (datas(10) != "null") {
          // 支付的场合
          val ids = datas(10).split(",")
          ids.foreach(
            id => {
              acc.add( (id, "pay") )
            }
          )
        }
      }
    )

    val accVal: mutable.Map[String, HotCategory] = acc.value
    val categories: mutable.Iterable[HotCategory] = accVal.map(_._2)

    val sort = categories.toList.sortWith(
      (left, right) => {
        if ( left.clickCnt > right.clickCnt ) {
          true
        } else if (left.clickCnt == right.clickCnt) {
          if ( left.orderCnt > right.orderCnt ) {
            true
          } else if (left.orderCnt == right.orderCnt) {
            left.payCnt > right.payCnt
          } else {
            false
          }
        } else {
          false
        }
      }
    )


    // 5. 将结果采集到控制台打印出来
    sort.take(10)
  }
}
