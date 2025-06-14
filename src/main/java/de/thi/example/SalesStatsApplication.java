package de.thi.example;

import de.thi.example.model.Sales;
import de.thi.example.model.SalesStats;
import de.thi.example.serdes.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class SalesStatsApplication {
    private static final Logger log = LoggerFactory.getLogger(SalesStatsApplication.class);

    //0. 定义一些常量
    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String APPLICATION_ID = "sales_stats_app_id";
    private static final String SALES_STATS_SOURCE_TOPIC = "sales.stats.topic";


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
        //1. source processor  这里的输入数据是Sales
        KStream<String, Sales> ks0 = builder.stream(SALES_STATS_SOURCE_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.Sales())
                .withName("salesstatus-source-processor")
                .withOffsetResetPolicy(AutoOffsetReset.latest())
        );

        //首先对销售数据进行分组
        ks0.groupBy((key, value) -> value.getDepartment(),
                        Grouped.with(Serdes.String(), JsonSerdes.Sales()))
                //进行aggregate操作 第一个参数是输出类型 第二个参数分别是Key,Value,Value Return
                .aggregate(SalesStats::new, (department, sales, salesstats) -> {
                            if (salesstats.getDepartment() == null) {
                                //说明是这个部门的首条记录
                                salesstats.setDepartment(department);
                                salesstats.setCount(1);
                                salesstats.setAverageSalesAmount(sales.getSalesAmount());
                                salesstats.setTotalSalesAmount(sales.getSalesAmount());
                            } else {
                                //计算销量
                                salesstats.setCount(salesstats.getCount() + 1);
                                //计算总销售额
                                salesstats.setTotalSalesAmount(salesstats.getTotalSalesAmount() + sales.getSalesAmount());
                                //计算销售单价
                                salesstats.setAverageSalesAmount(salesstats.getTotalSalesAmount() / salesstats.getCount());
                            }
                            return salesstats;
                        }, Named.as("sales-stats-aggregate-processor"),
                        //这里需要指定序列化的类型
                        Materialized.<String, SalesStats, KeyValueStore<Bytes, byte[]>>as("sales-stats")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(JsonSerdes.SalesStats())
                ).toStream().print(Printed.<String, SalesStats>toSysOut().withLabel("sales-stats-print-processor"))
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


    private static Sales populateSales(Sales sales) {
        if (sales.getSalesAmount() != sales.getTotalSalesAmount()) {
            sales.setTotalSalesAmount(sales.getSalesAmount());
        }
        return sales;
    }
}
