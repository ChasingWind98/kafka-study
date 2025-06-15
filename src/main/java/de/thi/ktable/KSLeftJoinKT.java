package de.thi.ktable;

import de.thi.example.model.Sales;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class KSLeftJoinKT {
    private static final Logger log = LoggerFactory.getLogger(KSLeftJoinKT.class);

    //0. 定义一些常量
    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String APPLICATION_ID = "ks_leftjoin_kt_app_id";
    private static final String KS_LEFT_JOIN_KT_STATE_STORE = "ks_lj_kt_statestore";
    //消息结构
    private static final String USER_INFO_TOPIC = "users.info";
    private static final String ADDRESS_INFO_TOPIC = "address.info";


    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        //设置StateStore的位置 默认是 /tmp/kafka-streams
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/chasingwind/Kafka/statestore");
        //设置线程数 跟Partition数一致
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);


        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> ks = builder.stream(USER_INFO_TOPIC, Consumed.with(Serdes.String(),
                        Serdes.String()).withName("user-source")
                .withOffsetResetPolicy(AutoOffsetReset.latest())
        );

        KTable<String, String> kt = builder.table(ADDRESS_INFO_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String()).withName("address-source")
                        .withOffsetResetPolicy(AutoOffsetReset.latest()),
                Materialized.as(KS_LEFT_JOIN_KT_STATE_STORE));

        ks.leftJoin(kt, (key, ksv, ktv) -> ksv + " come from " + (ktv == null ? "N/A" : ktv))
                .print(Printed.<String, String>toSysOut().withLabel("ktable-print"));


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
