package de.thi.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * 演示Map操作
 */
public class StreamsMapApplication {

    private static final Logger log = LoggerFactory.getLogger(StreamsMapApplication.class);

    //0. 定义一些常量
    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";
    private static final String APPLICATION_ID = "map_streams_app_id";

    public static void main(String[] args) throws InterruptedException {
        //1. Kafka  Streams Configuration
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);

        //2. create KafkaBuilder
        //null,kafak -> kafka,5  将value转换为键,并且计算长度作为Value
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> ks1 =
                builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String())
                        .withName("source-processor")
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));

        KStream<String, Integer> ks2 = ks1.map((nokey, value) -> KeyValue.pair(value, value.length()),Named.as("map-processor"));
        ks2.print(Printed.<String, Integer>toSysOut().withLabel("map2-streams-print"));


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
