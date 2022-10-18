package com.atguigu.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql._

object SparkSQL_JDBC {

  def main(args: Array[String]): Unit = {

    // TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // 读取MySQL数据
    val df = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://hdp103:3306/gmall?characterEncoding=utf-8&useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "199037")
      .option("dbtable", "user_info")
      .load()
    //df.show

    // 保存数据
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://hdp103:3306/gmall?characterEncoding=utf-8&useSSL=false")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "199037")
      .option("dbtable", "user_info1")
      .mode(SaveMode.Append)
      .save()


    // TODO 关闭环境
    spark.close()
  }
}

