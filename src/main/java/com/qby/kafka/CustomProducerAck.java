package com.qby.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class CustomProducerAck {

    public static void main(String[] args) throws ExecutionException,
            InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "172.16.13.150:9092");//kafka 集群，broker-list
        props.put("acks", "all");
        props.put("retries", 1);//重试次数
        props.put("batch.size", 16384);//批次大小
        props.put("linger.ms", 1);//等待时间
        props.put("buffer.memory", 33554432);//RecordAccumulator 缓冲区大小
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new
                KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            //回调函数，该方法会在 Producer 收到 ack 时调用，为异步调用
            producer.send(new ProducerRecord<String, String>("newt", 0,
                    Integer.toString(i), Integer.toString(i)), (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("success->offset:" +
                            metadata.offset() + " partition:" + metadata.partition()
                            + " topic:" + metadata.topic());
                } else {
                    exception.printStackTrace();
                }
            });
        }
        producer.close();
    }
}
