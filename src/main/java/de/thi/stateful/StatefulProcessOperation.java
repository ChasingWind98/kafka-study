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

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class StatefulProcessOperation {

    private static final Logger log = LoggerFactory.getLogger(StatefulProcessOperation.class);

    //0. 定义一些常量
    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String INPUT_TOPIC = "word.count.input.topic";
    private static final String OUTPUT_TOPIC = "word.count.output.topic";
    private static final String APPLICATION_ID = "stateful_word_count_app_id";
    private static final String STATE_STORE_NAME = "stateful_process_operation";

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


        //     定义statestore 其中key是单词 value是单词出现的个数
        StoreBuilder<KeyValueStore<String, Integer>> keyValueStoreStoreBuilder =
                //这里选择的是Persistent存储  也可以选择InMemory存储  还需要指定Key和Value的序列化器
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(STATE_STORE_NAME), Serdes.String(), Serdes.Integer());
        builder.addStateStore(keyValueStoreStoreBuilder);

        //<null, hello kafka>
        KStream<String, String> ks1 =
                builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.String())
                        .withName("source-processor")
                        .withOffsetResetPolicy(AutoOffsetReset.latest()));


        //Consume 数据
        //<null,hello kafka> --> <hello,hello> <kafka,kafka>
        ks1.flatMap((key, value) -> Arrays.stream(value.split("\\s+"))
                        .map(element -> KeyValue.pair(element, element)).collect(Collectors.toList()), Named.as("flatmap-processor"))
                //手动调用Repartition 这样就能将相同key的数据Repartition到一个分区
                .repartition(Repartitioned.with(Serdes.String(), Serdes.String()))
                //定义Processor  在这里可以使用State Store来记录count
                .process(() -> new Processor<String, String, String, Integer>() {

                    ProcessorContext<String, Integer> context;
                    private KeyValueStore<String, Integer> kvStore;

                    @Override
                    public void init(ProcessorContext<String, Integer> context) {
                        this.context = context;
                        this.kvStore = context.getStateStore(STATE_STORE_NAME);
                    }

                    @Override
                    public void process(Record<String, String> record) {


                        Integer count = kvStore.get(record.key());
                        if (count == null || count == 0) {
                            count = 1;
                        } else {
                            count++;
                        }
                        //更新count
                        kvStore.put(record.key(), count);
                        context.forward(new Record<>(record.key(), count, record.timestamp()));
                    }

                }, Named.as("stateful-customer-processor"), STATE_STORE_NAME)
                .peek((k, v) -> log.info("key: {} : count: {}", k, v))
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Integer()).withName("sink-processor"))
        ;


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
