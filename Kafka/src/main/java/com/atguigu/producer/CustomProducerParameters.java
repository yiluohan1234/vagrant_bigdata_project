package com.atguigu.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerParameters {

    public static Properties initConfig() {
        Properties properties = new Properties();
        // kafka 配置对象添加配置信息：bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp101:9092,hpd102:9092,hdp103:9092");
        // key,value 序列化（必须）
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // RecordAccumulator：缓冲区大小，默认 32M：buffer.memory
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        // batch.size：批次大小，默认 16K
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        // linger.ms：等待时间，默认 0
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        // compression.type：压缩，默认 none，可配置值 gzip、snappy、lz4 和 zstd
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        return properties;
    }

    public static void main(String[] args) {
        // 0创建配置
        Properties properties = initConfig();

        // 1 创建生产者
        KafkaProducer<Object, Object> kafkaProducer = new KafkaProducer<>(properties);

        for (int i = 0; i < 5; i++) {
            // 2 发送数据
            kafkaProducer.send(new ProducerRecord<>("first", "kafka"+i));
        }

        // 3 关闭资源
        kafkaProducer.close();
    }
}
