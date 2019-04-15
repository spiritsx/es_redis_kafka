package com.sx.kafka.adminClient;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.common.config.ConfigResource;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * created at 2019/4/13
 *
 * @author shixi
 */
public class KafkaAdminClientDemo {


    public static AdminClient initConfig() {
        String brokerList = "47.107.164.213:9092";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        return AdminClient.create(props);
    }

    public static void main(String[] args) {
        AdminClient client = initConfig();
        String topic = "topic-admin";
//        NewTopic newTopic = new NewTopic(topic, 4, (short) 1);

//        ListTopicsResult result = client.listTopics();
        try {
//        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));
//            Void aVoid = result.all().get();
//            System.out.println(aVoid);


//            KafkaFuture<Collection<TopicListing>> listings = result.listings();
//            Collection<TopicListing> topicListings = listings.get();
//            for (TopicListing topicListing : topicListings) {
//                System.out.println(topicListing.name());
//            }


//            DeleteTopicsResult deleteTopicsResult = client.deleteTopics(Collections.singleton(topic));
//            Void aVoid = deleteTopicsResult.all().get();
//            System.out.println(aVoid);


//            DescribeTopicsResult describeTopicsResult = client.describeTopics(Arrays.asList(topic));
//            Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
//            for (String s : stringTopicDescriptionMap.keySet()) {
//                System.out.println(s + "====>>>>" + stringTopicDescriptionMap.get(s));
//            }

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            DescribeConfigsResult result = client.describeConfigs(Collections.singleton(resource));
            Config config = result.all().get().get(resource);
            System.out.println(JSONObject.toJSONString(config));
            client.close();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        client.close();
    }
}
