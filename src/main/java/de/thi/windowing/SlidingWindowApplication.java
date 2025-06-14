package de.thi.windowing;

import de.thi.example.model.NetTraffic;
import de.thi.example.model.Sales;
import de.thi.example.serdes.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class SlidingWindowApplication {
    private static final Logger log = LoggerFactory.getLogger(SlidingWindowApplication.class);

    //0. 定义一些常量
    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String APPLICATION_ID = "sliding_window_app_id";
    private static final String SALES_STATS_SOURCE_TOPIC = "net.traffic.logs.topic";


    public static void main(String[] args) throws InterruptedException {
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


        StreamsBuilder builder = new StreamsBuilder();
        //1. source processor  这里的输入数据是Sales
        KStream<String, NetTraffic> ks0 = builder.stream(SALES_STATS_SOURCE_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.NetTraffic())
                .withName("sliding-window-source-processor")
                .withOffsetResetPolicy(AutoOffsetReset.latest())
        );
        ks0.groupBy((k, v) -> v.getUrl(), Grouped.with(Serdes.String(), JsonSerdes.NetTraffic()))
//                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)).advanceBy(Duration.ofSeconds(10)))
                //表示的是如果1分钟之内没有变化 就不会向后滑动创建新窗口  第二个参数表示Grace 不会延迟处理数据
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(0)))
                .count(Named.as("sliding-window-count-processor"), Materialized.as("sliding-window-count-store"))
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .peek((k, v) -> log.info("[{} --- {}]WebSeit :{} 在1分钟内被访问 {} 次", k.window().start(), k.window().end(), k.key(), v), Named.as("peek-processor"));



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


    private static Sales populateSales(Sales sales) {
        if (sales.getSalesAmount() != sales.getTotalSalesAmount()) {
            sales.setTotalSalesAmount(sales.getSalesAmount());
        }
        return sales;
    }
}
