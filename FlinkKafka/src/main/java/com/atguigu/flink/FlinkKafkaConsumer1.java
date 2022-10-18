package com.atguigu.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class FlinkKafkaConsumer1 {
    public static void main(String[] args) throws Exception {
        // 0 初始化 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 2 kafka生产配置信息
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp101:9092");

        // 3 创建 kafka 消费者
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "first",
                new SimpleStringSchema(),
                properties
        );

        // 3 消费者和 flink 流关联
        env.addSource(kafkaConsumer).print();

        // 4 执行
        env.execute();
    }
}
