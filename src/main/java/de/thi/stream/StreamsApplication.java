package de.thi.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamsApplication {

    private static final Logger log = LoggerFactory.getLogger(StreamsApplication.class);

    //0. 定义一些常量
    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";
    private static final String APPLICATION_ID = "first_streams_app_id";

    public static void main(String[] args) throws InterruptedException {
        //1. Kafka  Streams Configuration
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);

        //2. create KafkaBuilder
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("source-processor"))
                .peek((k, v) -> log.info("key:{}, value:{}", k, v), Named.as("pre-transform-peek-processor"))
                .filter(((k, v) -> v != null && v.length() > 5), Named.as("filter-processor"))
                .mapValues(v -> v.toUpperCase(), Named.as("mapvalues-processor"))
                .peek((k, v) -> log.info("key:{}, value:{}", k, v), Named.as("post-transform-peek-processor"))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()).withName("sink-processor"));

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
