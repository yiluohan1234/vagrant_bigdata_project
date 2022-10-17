package com.cuiyf41.spark.streaming

import com.cuiyf41.spark.util.MyKafkaSink

import java.util.{Properties, Random}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object SparkStreaming10_MockData {

    def main(args: Array[String]): Unit = {

        // 生成模拟数据
        // 格式 ：timestamp area city userid adid
        // 含义： 时间戳   区域  城市 用户 广告
        while (true) {
            mockdata().foreach(
                data => {
                    // 向Kafka中生成数据
                    MyKafkaSink.send("mock_data", data)
                    println(data)
                }
            )
            Thread.sleep(2000)
        }
    }
    def mockdata() = {
        val list = ListBuffer[String]()
        val areaList = ListBuffer[String]("华北", "华东", "华南")
        val cityList = ListBuffer[String]("北京", "上海", "深圳")

        for ( i <- 1 to new Random().nextInt(50) ) {
            val area = areaList(new Random().nextInt(3))
            val city = cityList(new Random().nextInt(3))
            var userid = new Random().nextInt(6) + 1
            var adid = new Random().nextInt(6) + 1

            list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${adid}")
        }
        list
    }
}
