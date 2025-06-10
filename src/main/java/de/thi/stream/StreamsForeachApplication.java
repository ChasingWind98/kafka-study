package de.thi.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Printed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * 演示Map操作
 */
public class StreamsForeachApplication {

    private static final Logger log = LoggerFactory.getLogger(StreamsForeachApplication.class);

    //0. 定义一些常量
    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";
    private static final String APPLICATION_ID = "foreach_streams_app_id";

    public static void main(String[] args) throws InterruptedException {
        //1. Kafka  Streams Configuration
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);

        //2. create KafkaBuilder
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> ks1 =
                builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String())
                        .withName("source-processor")
                        .withOffsetResetPolicy(Topology.AutoOffsetReset.LATEST));

        //terminal stream --> return void
        ks1.foreach((key, value) -> {
            System.out.println("key:" + key + ", value:" + value);
        }, Named.as("foreach-processor"));


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
