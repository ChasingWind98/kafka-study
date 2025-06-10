package de.thi.admin;

import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.ExecutionException;


public class AdminClientApp {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = AdminClientApp.adminClientApp()) {
            incrPartitions(adminClient);

        }
    }

    public static void incrPartitions(AdminClient adminClient) throws ExecutionException, InterruptedException {

        NewPartitions newPartitions = NewPartitions.increaseTo(3);
        adminClient.createPartitions(Map.of("my-topic", newPartitions));

    }

    public static void describeTopic(AdminClient adminClient) throws ExecutionException, InterruptedException {
        Collection<String> topics = Arrays.asList("my-topic");
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topics);
        Map<String, TopicDescription> topicsMap = describeTopicsResult.allTopicNames().get();
        topicsMap.forEach((key, value) -> {
            System.out.println("key = " + key);
            System.out.println("value = " + value);
        });
    }


    public static void listTopics(AdminClient adminClient) throws ExecutionException, InterruptedException {
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> names = listTopicsResult.names().get();
        names.stream().forEach(System.out::println);

    }

    public static void createTopic(AdminClient adminClient) {
        CreateTopicsResult topics = adminClient.createTopics(
                Arrays.asList(
                        //参数分别是 topic名称，分区数，副本数
                        new NewTopic("my-topic", 1, (short) 1)
                )
        );
        System.out.println("topics = " + topics.values());

    }

    public static AdminClient adminClientApp() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        return AdminClient.create(properties);
    }
}
