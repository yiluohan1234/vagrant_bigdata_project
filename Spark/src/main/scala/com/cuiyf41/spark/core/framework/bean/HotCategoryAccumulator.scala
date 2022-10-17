package com.cuiyf41.spark.core.framework.bean


import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
 * 自定义累加器
 * 1. 继承AccumulatorV2，定义泛型
 *    IN : ( 品类ID, 行为类型 )
 *    OUT : mutable.Map[String, HotCategory]
 * 2. 重写方法（6）
 */
class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]{

  private val hcMap = mutable.Map[String, HotCategory]()

  override def isZero: Boolean = {
    hcMap.isEmpty
  }

  override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
    new HotCategoryAccumulator()
  }

  override def reset(): Unit = {
    hcMap.clear()
  }

  override def add(v: (String, String)): Unit = {
    val cid = v._1
    val actionType = v._2
    val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
    if ( actionType == "click" ) {
      category.clickCnt += 1
    } else if (actionType == "order") {
      category.orderCnt += 1
    } else if (actionType == "pay") {
      category.payCnt += 1
    }
    hcMap.update(cid, category)
  }

  override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
    val map1 = this.hcMap
    val map2 = other.value

    map2.foreach{
      case ( cid, hc ) => {
        val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0,0,0))
        category.clickCnt += hc.clickCnt
        category.orderCnt += hc.orderCnt
        category.payCnt += hc.payCnt
        map1.update(cid, category)
      }
    }
  }

  override def value: mutable.Map[String, HotCategory] = hcMap
}
