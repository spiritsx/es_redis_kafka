package com.sx.kafka.producer;

import com.sx.kafka.bean.Company;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * created at 2019/4/11 0011
 *
 * @author shixi
 */
public class ProducerInterceptorPrefixPlus implements ProducerInterceptor<String, Company> {

    @Override
    public ProducerRecord<String, Company> onSend(ProducerRecord<String, Company> record) {
        return ProducerInterceptorPrefix.getStringCompanyProducerRecord(record);
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
