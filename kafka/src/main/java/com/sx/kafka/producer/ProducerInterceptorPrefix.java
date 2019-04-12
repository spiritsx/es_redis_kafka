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
public class ProducerInterceptorPrefix implements ProducerInterceptor<String, Company> {

    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    @Override
    public ProducerRecord<String, Company> onSend(ProducerRecord<String, Company> record) {
        return getStringCompanyProducerRecord(record);
    }

    static ProducerRecord<String, Company> getStringCompanyProducerRecord(ProducerRecord<String, Company> record) {
        Company company = record.value();
        company.setAddress("prefix-" + company.getAddress());
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(),
                record.key(), company, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            sendSuccess++;
        } else {
            sendFailure++;
            System.out.println("当前发送失败次数: " + sendFailure);
        }
    }

    @Override
    public void close() {
        double successRatio = (double) sendSuccess / (sendFailure + sendSuccess);
        System.out.println("[INFO] 发送成功率=" + String.format("%f", successRatio * 100) + "%");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
