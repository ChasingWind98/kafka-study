package de.thi.stateful;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class StatefulGroupAndAggreationOperation {

    private static final Logger log = LoggerFactory.getLogger(StatefulGroupAndAggreationOperation.class);

    //0. 定义一些常量
    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String APPLICATION_ID = "stateful_count_aggregation_app_id";

    //输入输出Topic
    private static final String WORD_INPUT_TOPIC = "word.count.input.topic";
    private static final String WORD_OUTPUT_TOPIC = "word.count.output.topic";


    public static void main(String[] args) throws InterruptedException {
        //1. Kafka  Streams Configuration
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        //设置StateStore的位置 默认是 /tmp/kafka-streams
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/chasingwind/Kafka/statestore");
        //设置线程数 跟Partition数一致
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);

        //2. create KafkaBuilder
        StreamsBuilder builder = new StreamsBuilder();
        //<null,this is kafka streams>
        //<null, kafka streams is so efficient>
        KStream<String, String> ks0 = builder.stream(WORD_INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String())
                .withName("source-processor")
                .withOffsetResetPolicy(AutoOffsetReset.latest())
        );
        //<this,1> <is,1>
        ks0.flatMap((k, v) ->
                        Arrays.stream(v.split(" ")).map((element) -> KeyValue.pair(element, 1L))
                                .collect(Collectors.toList()), Named.as("flatmap-processor"))
                //注意这里的groupByKey会导致RePartition 并且返回值类型是KGroupedStream
                // 且由于RePartition到Internal Topic,因此需要指定Key和Value的序列化器
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
                //数据需要保存到哪里?-->Materialized.as  本地的存储以及远程的Topic名字都是这个名字  同时可以看到类型是KTable,可以insert update
                .count(Named.as("word-count"), Materialized.as("word-count"))
                //将Ktable转换为KStream
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel("word-count"));


        //3. create Topology
        Topology topology = builder.build();
        //4. create KafkaStreams
        KafkaStreams streams = new KafkaStreams(topology, properties);

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().

                addShutdownHook(new Thread(() ->

                {
                    streams.close();
                    latch.countDown();
                    log.info("Streams closed");
                }));

        //5. start KafkaStream
        streams.start();
        log.info("Streams started");
        //6. graceful shutdown
        latch.await();

    }
}
