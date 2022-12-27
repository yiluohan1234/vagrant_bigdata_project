package com.atguigu.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerCallbackPartitions {

    public static Properties initConfig() {
        // 0配置
        Properties properties = new Properties();
        // 连接集群 bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp101:9092,hdp102:9092,hdp103:9092");
        // 指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }
    public static void main(String[] args) throws InterruptedException {
        // 0配置
        Properties properties = initConfig();

        // 1创建kafka生产对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 2发送数据
        for (int i = 0; i < 5; i++) {
            // 依次指定 key 值为 a,b,f ，数据 key 的 hash 值与 3 个分区求余，分别发往 1、2、0
            kafkaProducer.send(new ProducerRecord<>("first", 1, "", "hello" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("主题：" + recordMetadata.topic() + " 分区：" + recordMetadata.partition());
                    } else {
                        e.printStackTrace();
                    }
                }
            });
            Thread.sleep(2);
        }
        // 3关闭资源
        kafkaProducer.close();

    }
}
