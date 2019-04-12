package com.sx.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * created at 2019/4/12 0012
 *
 * @author shixi
 */
public class ConsumerInterceptorTTL implements ConsumerInterceptor<String, String> {

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        long now = System.currentTimeMillis();
        Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords = new HashMap<>();
        for (TopicPartition topicPartition : records.partitions()) {
            List<ConsumerRecord<String, String>> aliveRecords = new ArrayList<>();
            for (ConsumerRecord<String, String> record : records.records(topicPartition)) {
                if (TimestampType.CREATE_TIME.equals(record.timestampType())) {
                    if ((now - record.timestamp()) < 10000) {
                        aliveRecords.add(record);
                    }
                }
            }
            if (!aliveRecords.isEmpty()) {
                newRecords.put(topicPartition, aliveRecords);
            }
        }

        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        for (Map.Entry<TopicPartition, OffsetAndMetadata> topicPartitionOffsetAndMetadataEntry : offsets.entrySet()) {
            TopicPartition key = topicPartitionOffsetAndMetadataEntry.getKey();
            OffsetAndMetadata value = topicPartitionOffsetAndMetadataEntry.getValue();
            System.out.println(key + ":" + value);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
