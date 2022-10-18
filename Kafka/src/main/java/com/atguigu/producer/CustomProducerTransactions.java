package com.atguigu.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerTransactions {

    public static Properties initConfig() {
        // 0配置
        Properties properties = new Properties();
        // 连接集群 bootstrap.servers
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hdp101:9092,hdp102:9092,hdp103:9092");
        // 指定对应的key和value的序列化类型
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 设置事务 id（必须），事务 id 任意起名
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,  "transaction_id_0");

        return properties;
    }

    public static void main(String[] args) {

        Properties properties = initConfig();

        // 1创建kafka生产对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        // 初始化事务
        kafkaProducer.initTransactions();
        // 开启事务
        kafkaProducer.beginTransaction();

        try{
            // 2发送数据
            for (int i = 0; i < 5; i++) {
                kafkaProducer.send(new ProducerRecord<>("first", "kafka"+i));
            }
            // 模拟失败
            int i = 1 / 0;
            // 提交事务
            kafkaProducer.commitTransaction();;
        } catch (Exception e) {
            // 终止事务
            kafkaProducer.abortTransaction();
        } finally {
            // 3关闭资源
            kafkaProducer.close();
        }

    }
}
