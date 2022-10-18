package com.atguigu.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerAcks {

    public static Properties initConfig() {
        // 0配置
        Properties properties = new Properties();
        // 连接集群 bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp101:9092,hdp102:9092,hdp103:9092");
        // 指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // acks
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        // 重试次数 retries，默认是 int 最大值，2147483647
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        return properties;
    }

    public static void main(String[] args) {

        Properties properties = initConfig();

        // 1创建kafka生产对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 2发送数据
        for (int i = 0; i < 5; i++) {
            kafkaProducer.send(new ProducerRecord<>("first", "kafka"+i));
        }
        // 3关闭资源
        kafkaProducer.close();

    }
}
