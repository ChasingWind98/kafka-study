package de.thi.windowing;

import de.thi.stream.StreamsApplication;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class WindowSuppressOperation {

    private static final Logger log = LoggerFactory.getLogger(WindowSuppressOperation.class);

    //0. 定义一些常量
    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String APPLICATION_ID = "word_count_app_id";
    private static final String WORD_COUNT__SOURCE_TOPIC = "word.count.topic";

    public static void main(String[] args) throws InterruptedException {
        //1. Kafka  Streams Configuration
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        //设置StateStore的位置 默认是 /tmp/kafka-streams
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/chasingwind/Kafka/statestore");
        //设置线程数 跟Partition数一致
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
        //这里取的是系统处理时间,避免由于网络波动导致顺序不对
        properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());

        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        //2. create KafkaBuilder
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> ks0 = builder.stream(WORD_COUNT__SOURCE_TOPIC, Consumed.with(Serdes.String(), Serdes.String())
                .withName("word-count-source-processor")
                .withOffsetResetPolicy(AutoOffsetReset.latest())
        );

        ks0.flatMap((k, v) ->
                        Arrays.stream(v.split(" ")).map(element -> KeyValue.pair(element, 1)).collect(Collectors.toList()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
                .count(Named.as("count-processor"), Materialized.as("count-store"))
                //窗口内数据处理完成之后,将数据缓存到内存中,等待窗口关闭
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .foreach((k, v) -> log.info("[Window start: {} ---- Window end: {}]:Key: {} Count:{}", k.window().start(), k.window().end(), k.key(), v), Named.as("foreach-processor"))
        ;

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
