package com.qby.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class PartitionerProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
// Kafka 服务端的主机名和端口号
        props.put("bootstrap.servers", "172.16.13.150:9092");
// 等待所有副本节点的应答
        props.put("acks", "all");
// 消息发送最大尝试次数
        props.put("retries", 0);
// 一批消息处理大小
        props.put("batch.size", 16384);
// 增加服务端请求延时
        props.put("linger.ms", 1);
// 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
// key 序列化
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
// value 序列化
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
// 自定义分区
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.qby.kafka.partition.MyPartitioner");


        Producer<String, String> producer = new
                KafkaProducer<>(props);
        for (int i = 0; i < 10; i++) {
            //回调函数，该方法会在 Producer 收到 ack 时调用，为异步调用
            producer.send(new ProducerRecord<String, String>("newt",
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
