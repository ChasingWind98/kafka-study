package de.thi.example;

import de.thi.example.model.Sales;
import de.thi.example.serdes.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class SalesChampionApplication {
    private static final Logger log = LoggerFactory.getLogger(SalesChampionApplication.class);

    //0. 定义一些常量
    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String APPLICATION_ID = "sales_champion_app_id9";
    private static final String SALES_CHAMPION_SOURCE_TOPIC = "sales.champion.topic";


    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        //设置StateStore的位置 默认是 /tmp/kafka-streams
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/chasingwind/Kafka/statestore");
        //设置线程数 跟Partition数一致
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);


        StreamsBuilder builder = new StreamsBuilder();
        //1. source processor
        KStream<String, Sales> ks0 = builder.stream(SALES_CHAMPION_SOURCE_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.Sales())
                .withName("sales-champion-source-processor")
                .withOffsetResetPolicy(AutoOffsetReset.latest())
        );

        //select+ groupingby
        ks0.mapValues(SalesChampionApplication::populateSales, Named.as("sales-amount-map-processor"))
                .groupBy((k, v) -> v.getUserName(), Grouped.with(Serdes.String(), JsonSerdes.Sales()))
                //计算个人的销售额
                .reduce((aggregateValue, currentValue) -> Sales.newBuilder(currentValue).accumulate(aggregateValue.getTotalSalesAmount()).build(),
                        Named.as("sales-amount-reduce-processor"), Materialized.as("sales-amount-state-store"))
                //得到部门下最大的销售额
                .toStream()
                .groupBy((k, v) -> v.getDepartment(), Grouped.with(Serdes.String(), JsonSerdes.Sales()))
                .reduce((aggregateValue, currentValue) -> currentValue.getTotalSalesAmount() > aggregateValue.getTotalSalesAmount() ? currentValue : aggregateValue,
                        Named.as("sales-amount-champion-processor"), Materialized.as("sales-amount-champion-state-store"))
                .toStream()
                .print(Printed.<String, Sales>toSysOut().withLabel("sales-amount-print-processor"));

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
