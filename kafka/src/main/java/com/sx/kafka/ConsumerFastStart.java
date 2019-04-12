package com.sx.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * created at 2019/4/10 0010
 *
 * @author shixi
 */
public class ConsumerFastStart {

    //    public static final String brokerList = "47.107.164.213:9092,47.107.164.213:9093,47.107.164.213:9094";
    public static final String brokerList = "47.107.164.213:9092";
    public static final String topic = "topic-demo";
    public static final String groupId = "group.demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static Properties initConfig() {
        Properties properties = new Properties();
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        properties.put("value.deserializer", CompanyDeserializer.class.getName());
        properties.put("value.deserializer", ProtostuffDeserializerDemo.class.getName());
        properties.put("bootstrap.servers", brokerList);
        properties.put("group.id", groupId);
        properties.put("client.id", "consumer.client.id.demo");
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        KafkaConsumer<String, Company> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        while (isRunning.get()) {
            ConsumerRecords<String, Company> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, Company> record : records.records(topic)) {
                System.out.println("topic=" + record.topic() + ", partition= " + record.partition() + ", offset=" + record.offset());
                System.out.println("key=" + record.key() + ", value=" + record.value());
            }
//            for (TopicPartition partition : records.partitions()) {
//                for (ConsumerRecord<String, Company> record : records.records(partition)) {
//                    System.out.println("topic=" + record.topic() + ", partition= " + record.partition() + ", offset=" + record.offset());
//                    System.out.println("key=" + record.key() + ", value=" + record.value());
//                }
//            }
        }
    }
}
