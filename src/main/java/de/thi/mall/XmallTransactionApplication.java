package de.thi.mall;

import de.thi.mall.model.Transaction;
import de.thi.mall.model.TransactionKey;
import de.thi.mall.model.TransactionPattern;
import de.thi.mall.model.TransactionReward;
import de.thi.mall.serdes.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class XmallTransactionApplication {
    private static final Logger log = LoggerFactory.getLogger(XmallTransactionApplication.class);

    //0. 定义一些常量
    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String APPLICATION_ID = "xmall_app_id";
    private static final String XMALL_TRANSACTION_SOURCE_TOPIC = "xmall.transaction";
    private static final String XMALL_TRANSACTION_PATTERN_TOPIC = "xmall.pattern.transaction";
    private static final String XMALL_TRANSACTION_REWARD_TOPIC = "xmall.reward.transaction";
    private static final String XMALL_TRANSACTION_PURCHASES_TOPIC = "xmall.purchases.transaction";
    private static final String XMALL_TRANSACTION_COFFEE_TOPIC = "xmall.coffee.transaction";
    private static final String XMALL_TRANSACTION_ELECTIC_TOPIC = "xmall.electic.transaction";

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);

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
        //1.3 extract data to reward
        ks1.mapValues(value ->
                                TransactionReward.newBuilder(value).build(),
                        Named.as("transaction-reward"))
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
