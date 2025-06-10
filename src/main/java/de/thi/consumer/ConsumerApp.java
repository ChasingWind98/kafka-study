package de.thi.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class ConsumerApp {
    public static final String MY_TOPIC = "my-topic";

    public static void main(String[] args) {
        committedOffsetWithPartition2();
    }

    public static void consumeMessages() {
        //配置项
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:19092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //Consumer 是基于订阅的模式  这里可以订阅一个或者多个Topic的数据
        consumer.subscribe(Arrays.asList(MY_TOPIC));
        while (true) {
            //每隔100ms拉取一次数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n",
                        record.partition(), record.offset(), record.key(), record.value());
        }


    }

    public static void committedOffset() {
        //配置项
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:19092");
        props.setProperty("group.id", "test");
        //关闭自动提交
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.offset.reset", "earliest"); // 从最早的消息开始消费
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //Consumer 是基于订阅的模式  这里可以订阅一个或者多个Topic的数据
        consumer.subscribe(Arrays.asList(MY_TOPIC));
        while (true) {
            //每隔100ms拉取一次数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<String, String> record : records) {

                System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n",
                        record.partition(), record.offset(), record.key(), record.value());

                //如果失败 则回滚 不提交Offset
            }
            // 手动提交偏移量
            try {
                consumer.commitSync(); // 使用同步提交确保提交成功
                System.out.println("成功提交偏移量");
            } catch (Exception e) {
                System.err.println("提交偏移量失败: " + e.getMessage());
            }
        }
    }

    public static void committedOffsetWithPartition() {
        //配置项
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:19092");
        props.setProperty("group.id", "test");
        //关闭自动提交
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.offset.reset", "earliest"); // 从最早的消息开始消费
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //Consumer 是基于订阅的模式  这里可以订阅一个或者多个Topic的数据
        consumer.subscribe(Arrays.asList(MY_TOPIC));
        while (true) {
            //每隔100ms拉取一次数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

            for (TopicPartition partition : records.partitions()) {
                //获取每隔分区的数据
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                //每隔分区的数据单独处理
                for (ConsumerRecord<String, String> record : partitionRecords) {
                    System.out.println("topic:" + record.topic() + " partition:" + record.partition() + " offset:" + record.offset() + " key:" + record.key() + " value:" + record.value());
                }
                //手动提交分区的偏移量  以便下一次进行消费
                //获取当前分区记录的偏移量
                long offset = partitionRecords.get(partitionRecords.size() - 1).offset();
                //注意这里的offset需要+1,避免重复消费
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(offset + 1)));
                System.out.println("==============partition " + partition.partition() +" 消费结束====================");
            }
        }
    }
    public static void committedOffsetWithPartition2() {
        //配置项
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:19092");
        props.setProperty("group.id", "test");
        //关闭自动提交
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.offset.reset", "earliest"); // 从最早的消息开始消费
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //订阅Partition
        TopicPartition p0 = new TopicPartition(MY_TOPIC, 0);
        TopicPartition p1 = new TopicPartition(MY_TOPIC, 1);
        consumer.assign(Arrays.asList(p0, p1));

        consumer.seek(p0, 900);

        while (true) {
            //每隔100ms拉取一次数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));

            for (TopicPartition partition : records.partitions()) {
                //获取每隔分区的数据
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                //每隔分区的数据单独处理
                for (ConsumerRecord<String, String> record : partitionRecords) {
                    System.out.println("topic:" + record.topic() + " partition:" + record.partition() + " offset:" + record.offset() + " key:" + record.key() + " value:" + record.value());
                }
                //手动提交分区的偏移量  以便下一次进行消费
                //获取当前分区记录的偏移量
                long offset = partitionRecords.get(partitionRecords.size() - 1).offset();
                //注意这里的offset需要+1,避免重复消费
                consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(offset + 1)));
                System.out.println("==============partition " + partition.partition() +" 消费结束====================");
            }
        }
    }
}
