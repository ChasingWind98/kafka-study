package de.thi.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * 演示Map操作
 */
public class StreamsSpiltAndMergeApplication {

    private static final Logger log = LoggerFactory.getLogger(StreamsSpiltAndMergeApplication.class);

    //0. 定义一些常量
    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";
    private static final String APPLICATION_ID = "spilt_and_merge_streams_app_id";

    public static void main(String[] args) throws InterruptedException {
        //1. Kafka  Streams Configuration
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);

        //2. create KafkaBuilder
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> ks1 =
                builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String())
                        .withName("source-processor").withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));

        //这里的name很重要 因为会影响后面的stream
        //4个Stream的名字分别为 split-stream-Apache, split-stream-Kafka, split-stream-Stream, split-stream-default
        Map<String, KStream<String, String>> streamsMap = ks1.split(Named.as("split-stream-"))
                .branch((key, value) -> value.contains("Apache"), Branched.as("Apache"))
                .branch((key, value) -> value.contains("Kafka"), Branched.as("Kafka"))
                //在Branch的时候还可以对数据进行操作
                .branch((key, value) -> value.contains("Stream"), Branched
                        //注意这里如果修改name之后 后面需要对应修改
                        .withFunction(kstream -> kstream.mapValues(val -> val.toUpperCase()), "StreamUpperCase"))
                .defaultBranch(Branched.as("default"));

        //打印Streams的效果
        streamsMap.get("split-stream-StreamUpperCase").print(Printed.<String, String>toSysOut().withLabel("branch-streams-print"));

        //merge  需要注意的是 Merge的Stream Key和Value类型需要一致
        KStream<String, String> mergeStream = streamsMap.get("split-stream-Apache")
                .merge(streamsMap.get("split-stream-default"), Named.as("merge-processor"));
        //将合并的两个流进行Print
        mergeStream.print(Printed.<String, String>toSysOut().withLabel("merge-streams-print"));


        //3. create Topology
        Topology topology = builder.build();
        //4. create KafkaStreams
        KafkaStreams streams = new KafkaStreams(topology, properties);

        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
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
