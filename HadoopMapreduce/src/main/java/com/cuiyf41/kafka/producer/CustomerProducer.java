package com.cuiyf41.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class CustomerProducer {
    public static void main(String[] args) {
        Properties props = new Properties();

        //kafka 集群，broker-list
        props.put("bootstrap.servers",	"hdp101:9092");
        props.put("acks", "all");
        props.put("retries", 1);//重试次数
        props.put("batch.size", 16384);//批次大小
        props.put("linger.ms", 1);//等待时间
        props.put("buffer.memory",	33554432);//RecordAccumulator 缓冲区大小
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        //异步发送：不带回调函数
        for (int i = 0; i < 199; i++) {
            producer.send(new ProducerRecord<String, String>("first", "test_" + i));
        }
        //异步发送：带回调函数
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("first", "test_" + i), new Callback() {
                @Override
                //回调函数，该方法会在 Producer 收到 ack 时调用，为异步调用
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("success->" + recordMetadata.offset());
                    } else {
                        e.printStackTrace();
                    }
                }
            });
        }

        producer.close();

    }
}
