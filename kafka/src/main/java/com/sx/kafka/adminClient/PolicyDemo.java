package com.sx.kafka.adminClient;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

import java.util.Map;

/**
 * created at 2019/4/13
 *
 * @author shixi
 */
public class PolicyDemo implements CreateTopicPolicy {
    public static void main(String[] args) {

    }


    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        if (requestMetadata.numPartitions() != null ||
                requestMetadata.replicationFactor() != null) {
            if (requestMetadata.numPartitions() < 5) {
                throw new PolicyViolationException("Topic should have at least 5 partitions, received:" +
                        requestMetadata.numPartitions());
            }

            if (requestMetadata.replicationFactor() <= 1) {
                throw new PolicyViolationException("Topic should have at least 2replication factor, received: " +
                        requestMetadata.replicationFactor());
            }
        }
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}




