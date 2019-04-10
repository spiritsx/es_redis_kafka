package com.sx.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

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

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", CompanyDeserializer.class.getName());
        properties.put("bootstrap.servers", brokerList);
        properties.put("group.id", groupId);
        KafkaConsumer<String, Company> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
        while (true) {
            ConsumerRecords<String, Company> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, Company> record : records) {
                System.out.println(record.value());
            }
        }
    }
}
