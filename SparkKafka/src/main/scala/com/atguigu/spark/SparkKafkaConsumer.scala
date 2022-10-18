package com.atguigu.spark

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkKafkaConsumer {
  def main(args: Array[String]): Unit = {
    // 1.创建 SparkConf
    val sparkConf = new SparkConf().setAppName("sparkstreaming").setMaster("local[*]")

    // 2.创建 StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 3.定义 Kafka 参数：kafka 集群地址、消费者组名称、key 序列化、value 序列化
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hdp101:9092,hdp102:9092,hdp103:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "atguiguGroup",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    // 4.读取 Kafka 数据创建 DStream
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent, //优先位置
        ConsumerStrategies.Subscribe[String, String](Set("first"), kafkaPara)// 消费策略：（订阅多个主题，配置参数）
    )

    // 5.将每条消息的 KV 取出
    val valueDStream: DStream[String] = kafkaDStream.map(record => record.value())

    //6.计算 WordCount
    valueDStream.print()

    //7.开启任务
    ssc.start()
    ssc.awaitTermination()

  }

}
