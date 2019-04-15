package com.sx.kafka.producer;

import com.sx.kafka.bean.DemoPartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

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
//    public static final String topic = "topic-demo";
    public static final String topic = "msg_format_v2";
    public static final long EXPIRE_INTERVAL = 10 * 1000;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        properties.put("value.serializer", CompanySerializer.class.getName());
//        properties.put("value.serializer", ProtostuffSerializerDemo.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());

        properties.put("bootstrap.servers", brokerList);
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DemoPartitioner.class.getName());
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
//        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProducerInterceptorPrefix.class.getName() +
//                "," + ProducerInterceptorPrefixPlus.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

//        for (int i = 0; i < 5; i++) {
//            Company company = Company.builder().name("hiddenKafka-->" + i)
//                    .address("China").build();
//            ProducerRecord<String, Company> record = new ProducerRecord<>(topic,
//                    company);
//            //同步调用
////                RecordMetadata metadata = producer.send(record).get();
////                System.out.println(metadata.topic() + "-" + metadata.partition()
////                        + ":" + metadata.offset());
//            //异步调用
//            producer.send(record, (metadata, exception) -> {
//                if (exception != null) {
//                    exception.printStackTrace();
//                } else {
//                    System.out.println(metadata.topic() + "-" + metadata.partition()
//                            + ":" + metadata.offset());
//                }
//            });
//
//        }

        // 测试消费者过滤器
//        ProducerRecord<String, String> record = new ProducerRecord<>(topic, 0,
//                System.currentTimeMillis() - EXPIRE_INTERVAL, null, "first-expire-data");
//        producer.send(record).get();
//
//        ProducerRecord<String, String> record1 = new ProducerRecord<>(topic, 0,
//                System.currentTimeMillis(), null, "normal-data");
//        producer.send(record1).get();
//
//        ProducerRecord<String, String> record2 = new ProducerRecord<>(topic, 0,
//                System.currentTimeMillis() - EXPIRE_INTERVAL, null, "last-expire-data");
//        producer.send(record2).get();

        for (int i = 0; i < 6; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,"key",
                    "value");
            producer.send(record);
        }

        producer.close();
    }
}
