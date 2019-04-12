package com.sx.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * created at 2019/4/12 0012
 *
 * @author shixi
 */
public class ThreadPoolConsumerDemo {

    public static final String topic = "topic-demo";

    private static Properties initConfig() {
        return FirstMultiConsumerThreadDemo.initConfig();
    }

    public static void main(String[] args) {
        Properties properties = initConfig();
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        ConsumerThread consumerThread = new ConsumerThread(properties, topic,
                Runtime.getRuntime().availableProcessors());
        consumerThread.start();
    }

    private static class ConsumerThread extends Thread {
        private KafkaConsumer<String, String> kafkaConsumer;
        private static ExecutorService executorService;
        private static Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        ConsumerThread(Properties properties, String topic, int threadNum) {
            kafkaConsumer = new KafkaConsumer<>(properties);
            kafkaConsumer.subscribe(Collections.singletonList(topic));
            executorService = new ThreadPoolExecutor(threadNum, threadNum, 0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        }

        @Override
        public void run() {
            try {
                while (true) {
//                    System.out.println("调用线程池的线程：" + Thread.currentThread().getId());
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                    if (!records.isEmpty()) {
                        executorService.submit(new RecordsHandler(records));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
            }
        }

        private class RecordsHandler extends Thread {
            final ConsumerRecords<String, String> records;

            RecordsHandler(ConsumerRecords<String, String> records) {
                this.records = records;
            }

            @Override
            public void run() {
                for (TopicPartition topicPartition : records.partitions()) {
                    List<ConsumerRecord<String, String>> tpRecords = this.records.records(topicPartition);
                    long lastConsumedOffset = tpRecords.get(tpRecords.size() - 1).offset();
                    synchronized (offsets) {
                        if (!offsets.containsKey(topicPartition)) {
                            offsets.put(topicPartition, new OffsetAndMetadata(lastConsumedOffset + 1));
                        } else {
                            long position = offsets.get(topicPartition).offset();
                            if (position < lastConsumedOffset + 1) {
                                offsets.put(topicPartition, new OffsetAndMetadata(lastConsumedOffset + 1));
                            }
                        }
                    }
                    for (ConsumerRecord<String, String> record : records) {

                        System.out.println("线程池内的线程：" + Thread.currentThread().getId());
                        System.out.println("topic =" + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                    }

                    synchronized (offsets) {
                        if (!offsets.isEmpty()) {
                            kafkaConsumer.commitSync(offsets);
                            offsets.clear();
                        }
                    }
                }


                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("线程池内的线程：" + Thread.currentThread().getId());
                    System.out.println("topic =" + record.topic() + ", partition = " + record.partition() + ", offset = " + record.offset());
                }
            }
        }
    }
}
