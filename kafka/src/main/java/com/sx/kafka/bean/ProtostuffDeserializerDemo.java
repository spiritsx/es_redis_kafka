package com.sx.kafka.bean;

import com.sx.kafka.bean.Company;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * created at 2019/4/11 0011
 *
 * @author shixi
 */
public class ProtostuffDeserializerDemo implements Deserializer<Company> {

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public Company deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        Schema schema = RuntimeSchema.getSchema(Company.class);
        Company company = new Company();
        ProtostuffIOUtil.mergeFrom(data, company, schema);
        return company;
    }

    @Override
    public void close() {

    }
}
