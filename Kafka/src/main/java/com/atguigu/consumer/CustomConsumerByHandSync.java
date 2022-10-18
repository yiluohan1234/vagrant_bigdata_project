package com.atguigu.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class CustomConsumerByHandSync {

    public static Properties initConfig() {
        Properties properties = new Properties();

        // 配置 连接 bootstrap.server
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp101:9092,hdp102:9092,hdp103:9092");

        // 配置序列化（反序列化）
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 手动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // 配置消费者组（组名任意起名）
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");

        return properties;
    }

    public static void main(String[] args) {
        // 0 配置
        Properties properties = initConfig();

        // 1创建消费者
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        // 2订阅主题 first
        ArrayList<String> topics = new ArrayList<>();
        topics.add("first");

        kafkaConsumer.subscribe(topics);

        // 3消费数据
        while (true) {
            // 设置 1s 中消费一批数据
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));

            // 打印消费到的数据
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
            }

            // 手动提交offset（同步方式）
            kafkaConsumer.commitSync();
            // 手动提交offset（异步方式）
            kafkaConsumer.commitAsync();
        }
    }
}
