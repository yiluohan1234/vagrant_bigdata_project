package com.atguigu.spark.core.practice

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import java.util
import scala.collection.mutable


/**
 * 用户查询日志(SogouQ)分析，数据来源Sogou搜索引擎部分网页查询需求及用户点击情况的网页查询日志数据集合。
 * 1．搜索关键词统计，使用HanLP中文分词
 * 2．用户搜索次数统计
 * 3．搜索时间段统计*数据格式:访问时间\t用户ID\t[查询词]\t i该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL
 * 其中，用户ID是根据用户使用浏览器访问搜索引擎时的Cookie信息自动赋值，即同一次使用浏览器输入的不同查询对应同一个用户ID
 */

object SougouSearchLogAnalysis1 {
  /**
   * 用户搜索点击网页记录Record
   * @param queryTime  访问时间，格式为：HH:mm:ss
   * @param userId     用户ID
   * @param queryWords 查询词
   * @param resultRank 该URL在返回结果中的排名
   * @param clickRank  用户点击的顺序号
   * @param clickUrl   用户点击的URL
   */
  case class SogouRecord(
                          queryTime: String,
                          userId: String,
                          queryWords: String,
                          resultRank: Int,
                          clickRank: Int,
                          clickUrl: String
                        )
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf()
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // TODO: 1. 本地读取SogouQ用户查询日志数据。本地文件需要加file:///
    val rawLogsRDD: RDD[String] = sc.textFile("/datas/SogouQ.sample")
    //println(s"Count = ${rawLogsRDD.count()}")

    // TODO: 2. 解析数据，封装到CaseClass样例类中
    // 在scala中，\s是用来匹配任何空白字符，当\放在最前面，前面得再放个\，或者在scala中用"""\s+"""
    val SogouRecordRDD: RDD[SogouRecord] = rawLogsRDD
      // 过滤不合法数据，如null，分割后长度不等于6
      .filter(log => log != null && log.trim.split("\\s+").length == 6)
      // 对每个分区中数据进行解析，封装到SogouRecord
      .mapPartitions(iter => {
        iter.map(log => {
          val arr: Array[String] = log.trim.split("\\s+")
          SogouRecord(
            arr(0),
            arr(1),
            arr(2).replaceAll("\\[|\\]", ""),
            arr(3).toInt,
            arr(4).toInt,
            arr(5)
          )
        })
      })
    // println(s"Count = ${SogouRecordRDD.count()},\nFirst = ${SogouRecordRDD.first()}")

    // 数据使用多次，进行缓存操作，使用count触发
    SogouRecordRDD.persist(StorageLevel.MEMORY_AND_DISK).count()

    //TODO 3.1 =================== 3.1 搜索关键词统计 ===================
    // a. 获取搜索词，进行中文分词
    val wordsRDD: RDD[String] = SogouRecordRDD.mapPartitions(iter => {
      iter.flatMap(record => {
        // 使用HanLP中文分词库进行分词
        val terms: util.List[Term] = HanLP.segment(record.queryWords.trim)
        // 将Java中集合对转换为Scala中集合对象
        import scala.collection.JavaConverters._
        terms.asScala.map(_.word)
      })
    })
    // println(s"Count = ${wordsRDD.count()}, Example = ${wordsRDD.take(5).mkString(",")}")

    // b. 统计搜索词出现次数，获取次数最多Top10
    val top10SearchWords: Array[(Int, String)] = wordsRDD
      .filter(word => !word.equals(".") && !word.equals("+"))
      .map((_, 1)) // 每个单词出现一次
      .reduceByKey(_ + _) // 分组统计次数
      .map(_.swap)
      .sortByKey(ascending = false) // 词频降序排序
      .take(10) // 获取前10个搜索词

    //TODO 3.2 =================== 3.2 用户搜索点击次数统计 ===================
    /*
        每个用户在搜索引擎输入关键词以后，统计点击网页数目，反应搜索引擎准确度
     */
    val clickCountRDD: RDD[((String, String), Int)] = SogouRecordRDD
      .mapPartitions { iter =>
        iter.map { record =>
          val userID: String = record.userId
          val querywords: String = record.queryWords
          //组合用户的ID和queryword为key
          ((userID, querywords), 1)
        }
      }
      //分组聚合
      .reduceByKey(_+_)
    val top10clickCount: Array[((String, String), Int)] = clickCountRDD
      .sortBy(_._2, ascending = false)
      .take(10)
    //TODO :获取搜索的点击的次数的最大值和最小中 和平均值
//    println(s"Max Click Count = ${clickCountRDD.map(_._2).max()}")
//    println(s"Min Click Count = ${clickCountRDD.map(_._2).min()}")
//    println(s"Avg Click Count = ${clickCountRDD.map(_._2).mean()}")

    //TODO 3.3 =================== 3.3 搜索时间段统计 ===================
    /*
        从搜索时间字段获取小时，统计每个小时搜索次数
     */
    val hourSearchCount: Array[(String, Int)] = SogouRecordRDD.map(record => {
        // 提取小时和分钟
        // 03:12:50
        val timeStr: String = record.queryTime
        val hourAndMitunesStr: String = timeStr.substring(0, 5)
        (hourAndMitunesStr, 1)
      })
      .reduceByKey(_ + _) // 分组统计次数
      .sortBy(_._2, ascending = false)
      .take(10)

    //TODO 4.输出结果
    println("=================== 3.1 搜索关键词统计 ===================")
    top10SearchWords.foreach(println)
    println("=================== 3.2 用户搜索点击次数统计 ===================")
    top10clickCount.foreach(println)
    println("=================== 3.3 搜索时间段统计 ===================")
    hourSearchCount.foreach(println)

    //TODO 5. 释放资源
    sc.stop()
  }

}

