package de.thi.stateful;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class StatefulInnerJoinOperation {

    private static final Logger log = LoggerFactory.getLogger(StatefulInnerJoinOperation.class);

    //0. 定义一些常量
    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String APPLICATION_ID = "stateful_inner_join_app_id";

    //两个Topic
    private static final String NAME_INPUT_TOPIC = "name-input-topic";
    private static final String ADDRESS_INPUT_TOPIC = "address-input-topic";


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

        //NAME_INPUT_TOPIC   <null,001,Zhang San>
        //ADDRESS_INPUT_TOPIC <null, 001, Beijing>
        KStream<String, String> nameKs = builder.stream(NAME_INPUT_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
                        .withName("name-input-processor")
                        .withOffsetResetPolicy(AutoOffsetReset.latest())
        );

        KStream<String, String> addressKs = builder.stream(ADDRESS_INPUT_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
                        .withName("address-input-processor")
                        .withOffsetResetPolicy(AutoOffsetReset.latest())
        );

        //处理key和value <null,001,Zhang San> --> <001,Zhang San>
        KStream<String, String> nameKs1 = nameKs.map((key, value) -> KeyValue.pair(value.split(",")[0], value.split(",")[1]), Named.as("name-map-processor"));
        //<null,001,Beijing> --> <001,Beijing>
        KStream<String, String> addressKs1 = addressKs.map((key, value) -> KeyValue.pair(value.split(",")[0], value.split(",")[1]), Named.as("address-map-processor"));

        //开始InnerJoin
        nameKs1.join(addressKs1, //left 是namesKs1, right 是addressKs1  这两个KS进行inner JOin
                (name, address) -> name + " " + address,   //Innerjoin时候对值进行的操作
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1)),  //TimeWindow
                // //Join的结果会保存到StateStore和changelog中,所以需要知道如何序列化 第一个是Key的类型后面分别是两个值类型
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        ).print(Printed.<String, String>toSysOut().withLabel("InnerJoin"));

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
