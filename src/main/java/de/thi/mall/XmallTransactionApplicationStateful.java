package de.thi.mall;

import de.thi.mall.model.Transaction;
import de.thi.mall.model.TransactionKey;
import de.thi.mall.model.TransactionPattern;
import de.thi.mall.model.TransactionReward;
import de.thi.mall.serdes.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.api.*;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class XmallTransactionApplicationStateful {
    private static final Logger log = LoggerFactory.getLogger(XmallTransactionApplicationStateful.class);

    //0. 定义一些常量
    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String APPLICATION_ID = "stateful_xmall_app_id";
    private static final String XMALL_TRANSACTION_SOURCE_TOPIC = "xmall.transaction";
    private static final String XMALL_TRANSACTION_PATTERN_TOPIC = "xmall.pattern.transaction";
    private static final String XMALL_TRANSACTION_REWARD_TOPIC = "xmall.reward.transaction";
    private static final String XMALL_TRANSACTION_PURCHASES_TOPIC = "xmall.purchases.transaction";
    private static final String XMALL_TRANSACTION_COFFEE_TOPIC = "xmall.coffee.transaction";
    private static final String XMALL_TRANSACTION_ELECTIC_TOPIC = "xmall.electic.transaction";

    private static final String STATE_STORE_NAME = "xmall_stateful_statestore_name";


    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        //设置StateStore的位置 默认是 /tmp/kafka-streams
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/chasingwind/Kafka/statestore");
        //设置线程数 跟Partition数一致
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);


        StreamsBuilder builder = new StreamsBuilder();

        //1. source processor
        KStream<String, Transaction> ks0 = builder.stream(XMALL_TRANSACTION_SOURCE_TOPIC,
                Consumed.with(Serdes.String(), JsonSerdes.Transaction())
                        .withName("source-processor")
                        .withOffsetResetPolicy(AutoOffsetReset.latest()));

        //1.1 masking processor  后面的所有步骤都是基于ks1
        KStream<String, Transaction> ks1 = ks0.peek((k, v) -> log.info("pre masking, {}", v))
                .mapValues(v -> Transaction.newBuilder(v).maskCreditCardNumber().build(),
                        Named.as("transaction-masking-creditcard"));

        //1.2 extract data to pattern
        ks1.mapValues(value -> TransactionPattern.newBuilder(value).build(), Named.as("transaction-pattern"))
                .to(XMALL_TRANSACTION_PATTERN_TOPIC, Produced.with(Serdes.String(), JsonSerdes.TransactionPattern()));
        //1.3 extract data to reward  累加Reward
        ks1.mapValues(value -> TransactionReward.newBuilder(value).build(), Named.as("transaction-reward"))
                .selectKey((k, v) -> v.getCustomerId())
                .peek((k, v) -> log.info("pre reward, key: {}, value: {}", k, v))
                .repartition(Repartitioned.with(Serdes.String(), JsonSerdes.TransactionReward()))
                .processValues(new FixedKeyProcessorSupplier<String, TransactionReward, TransactionReward>() {

                    @Override
                    public FixedKeyProcessor<String, TransactionReward, TransactionReward> get() {
                        return new FixedKeyProcessor<String, TransactionReward, TransactionReward>() {

                            private FixedKeyProcessorContext<String, TransactionReward> context;
                            private KeyValueStore<String, Integer> kvStore;

                            @Override
                            public void init(FixedKeyProcessorContext<String, TransactionReward> context) {
                                this.context = context;
                                this.kvStore = context.getStateStore(STATE_STORE_NAME);
                            }

                            @Override
                            public void process(FixedKeyRecord<String, TransactionReward> record) {
                                //key 是customerID
                                //获取已经存储的用户积分
                                String customerId = record.key();
                                TransactionReward reward = record.value();
                                Integer rewardPoints = kvStore.get(customerId);
                                if (rewardPoints == null || rewardPoints == 0) {
                                    rewardPoints = reward.getRewardAmount();
                                } else {
                                    //否则加上积分
                                    rewardPoints = rewardPoints + reward.getRewardAmount();
                                }
                                kvStore.put(customerId, rewardPoints);
                                record.value().setRewardTotal(rewardPoints);
                                //一定不要忘记context.forward
                                context.forward(record);
                            }

                            @Override
                            public void close() {

                            }
                        };
                    }

                    @Override
                    public Set<StoreBuilder<?>> stores() {
                        return Set.of(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(STATE_STORE_NAME), Serdes.String(), Serdes.Integer()));
                    }

                }, Named.as("transaction-reward-processor"))
                .peek((k, v) -> log.info("post reward, key: {}, value: {}", k, v))
                .to(XMALL_TRANSACTION_REWARD_TOPIC, Produced.with(Serdes.String(), JsonSerdes.TransactionReward()));

        //1.4 filter 大额交易
        ks1.filter((k, v) -> v.getPrice() > 1000)
                //把序列化的TransactionKey作为key
                .selectKey((k, v) -> new TransactionKey(v.getCustomerId(), v.getPurchaseDate()), Named.as("transaction-reward-key"))
                .to(XMALL_TRANSACTION_PURCHASES_TOPIC, Produced.with(JsonSerdes.TransactionKey(), JsonSerdes.Transaction()));

        //1.5 拆分数据
        ks1.split(Named.as("transaction-split-"))
                .branch((k, v) -> "coffee".equals(v.getDepartment()), Branched.withConsumer(ks -> ks.to(XMALL_TRANSACTION_COFFEE_TOPIC, Produced.with(Serdes.String(), JsonSerdes.Transaction()))))
                .branch((k, v) -> "electic".equals(v.getDepartment()), Branched.withConsumer(ks -> ks.to(XMALL_TRANSACTION_ELECTIC_TOPIC, Produced.with(Serdes.String(), JsonSerdes.Transaction()))));

        ks1.foreach((k, v) -> log.info("simulation located data to db: {}", v));

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
