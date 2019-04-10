package com.sx.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * created at 2019/4/10 0010
 *
 * @author shixi
 */
public class ProducerFastStart {

    //    public static final String brokerList = "47.107.164.213:9092,47.107.164.213:9093,47.107.164.213:9094";
    public static final String brokerList = "47.107.164.213:9092";
    public static final String topic = "topic-demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", CompanySerializer.class.getName());
        properties.put("bootstrap.servers", brokerList);
        KafkaProducer<String, Company> producer = new KafkaProducer<>(properties);


        for (int i = 0; i < 5; i++) {
            Company company = Company.builder().name("hiddenKafka-->" + i)
                    .address("China").build();
            ProducerRecord<String, Company> record = new ProducerRecord<>(topic,
                    company);
            //同步调用
//                RecordMetadata metadata = producer.send(record).get();
//                System.out.println(metadata.topic() + "-" + metadata.partition()
//                        + ":" + metadata.offset());
            //异步调用
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    System.out.println(metadata.topic() + "-" + metadata.partition()
                            + ":" + metadata.offset());
                }
            });

        }


        producer.close();
    }
}
