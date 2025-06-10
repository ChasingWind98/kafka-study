package de.thi.stream;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 用于演示Kafka  Stream
 */
@Slf4j
public class StreamApp {
    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";

    public static void main(String[] args) throws InterruptedException {

        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.execute(() -> {
            try {
                sendData();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        executorService.execute(StreamApp::consumeData);
        executorService.execute(StreamApp::wordCount);


    }

    private static void consumeData() {
        //配置项
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:19092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        //Consumer 是基于订阅的模式  这里可以订阅一个或者多个Topic的数据
        consumer.subscribe(Arrays.asList(OUTPUT_TOPIC));
        while (true) {
            //每隔100ms拉取一次数据
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("key = %s, value = %s%n",
                        record.key(), record.value());
        }
    }

    private static void sendData() throws InterruptedException {
        //配置项
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        //序列化配置
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String value1 = "In 2023, Trump was found liable in civil cases for sexual abuse and defamation and for business fraud. He was found guilty of falsifying business records in 2024, making him the first U.S. president convicted of a felony. After winning the 2024 presidential election against Kamala Harris, he was sentenced to a penalty-free discharge, and two felony indictments against him for retention of classified documents and obstruction of the 2020 election were dismissed without prejudice. A racketeering case related to the 2020 election in Georgia is pending.";
        String value2 = "Trump began his second presidency by pardoning around 1,500 January 6 rioters and initiating mass layoffs of federal workers. He imposed tariffs on nearly all countries, including large tariffs on China, Canada, and Mexico. Many of his administration's actions—including intimidation of political opponents and civil society, deportations of immigrants, and extensive use of executive orders—have drawn over 300 lawsuits challenging their legality. High-profile cases have underscored his broad interpretation of the unitary executive theory and have led to significant conflicts with the federal courts.";
        String value3 = "Trump is the central figure of Trumpism, and his faction is dominant within the Republican Party. Many of his comments and actions have been characterized as racist or misogynistic, and he has made false and misleading statements and promoted conspiracy theories to a degree unprecedented in American politics. Trump's actions, especially in his second term, have been described as authoritarian and contributing to democratic backsliding. After his first term, scholars and historians ranked him as one of the worst presidents in American history.";
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "key1", value1));
            Thread.sleep(Duration.ofSeconds(5).toMillis());
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "key2", value2));
            Thread.sleep(Duration.ofSeconds(5).toMillis());
            producer.send(new ProducerRecord<>(INPUT_TOPIC, "key3", value3));
        }


        producer.close();
    }



    private static void wordCount() {
        //配置项
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-app");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        //Kafka Streams主对象
        StreamsBuilder builder = new StreamsBuilder();
        //创建KStream对象  从某个Topic读数据 然后进行流处理 最后写到某个Topic
        KStream<String, String> stream = builder.stream(INPUT_TOPIC);
        stream.flatMapValues(value -> Arrays.asList(value.toLowerCase().split(" ")))
                .groupBy((key, value) -> value)
                .count()
                .toStream()
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        //创建Kafka Streams  第一个参数是Topology 第二个参数是配置项
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, properties);
        System.out.println(topology.describe());

        //由于Streams是一个不间断地数据处理 所以需要启动一个线程来处理
        streams.start();
    }

}

