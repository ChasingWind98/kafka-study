package de.thi.ktable;

import de.thi.example.model.Employee;
import de.thi.example.model.Sales;
import de.thi.example.model.Shoot;
import de.thi.example.model.ShootStats;
import de.thi.example.serdes.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class TransformValuesOperation {
    private static final Logger log = LoggerFactory.getLogger(TransformValuesOperation.class);

    //0. 定义一些常量
    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String APPLICATION_ID = "shoot_app_id1";
    private static final String SHOOT_SOURCE_TOPIC = "shoot.topic";
    private static final String STATE_STORE_NAME = "shoot.statestore";


    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        //设置StateStore的位置 默认是 /tmp/kafka-streams
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/chasingwind/Kafka/statestore");
        //设置线程数 跟Partition数一致
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);

        StreamsBuilder builder = new StreamsBuilder();
        //首先构建一个StateStore
        StoreBuilder<KeyValueStore<String, ShootStats>> storeBuilder =
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(STATE_STORE_NAME), Serdes.String(), JsonSerdes.ShootStats());
        builder.addStateStore(storeBuilder);


        KTable<String, Shoot> kt = builder.table(SHOOT_SOURCE_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.Shoot())
                .withName("shoot-tabel-processor")
                .withOffsetResetPolicy(AutoOffsetReset.latest())
        );
        kt.transformValues(() -> new ValueTransformerWithKey<String, Shoot, ShootStats>() {

                    private KeyValueStore<String, ShootStats> store;

                    @Override
                    public void init(ProcessorContext context) {
                        //声明上下文中需要使用的StateStore
                        this.store = context.getStateStore(STATE_STORE_NAME);
                    }

                    @Override
                    public ShootStats transform(String playerName, Shoot shoot) {
                        log.info("Transforming record: key={}, value={}", playerName, shoot);
                        //首先 如果value是null 我们直接返回给null 给下游-->可以看到这里与其他KTable的操作不一样 Value为null不代表删除语义
                        //所以在transformValues方法中需要独自处理null
                        if (shoot == null) {
                            return null;
                        }
                        //如果玩家是第一次玩
                        ShootStats shootStats = this.store.get(playerName);
                        if (shootStats == null) {
                            //将第一次的数据存储到缓存中
                            shootStats =  ShootStats.Builder.newBuilder()
                                    .playerName(playerName)
                                    .count(1)
                                    .bestScore(shoot.getScore())
                                    .lastScore(shoot.getScore())
                                    .status("PROCESSING")
                                    .build();

                        } else if (Objects.equals(shootStats.getStatus(), "FINISHED")) {
                            //如果已经10次,那么就不会更新
                            return shootStats;
                        } else {
                            shootStats.setCount(shootStats.getCount() + 1);
                            shootStats.setLastScore(shoot.getScore());
                            shootStats.setBestScore(Math.max(shoot.getScore(), shootStats.getBestScore()));
                            if (shootStats.getCount() > 10) {
                                shootStats.setStatus("FINISHED");
                            }
                        }
                        //不要忘记把数据保存到缓存中
                        this.store.put(shoot.getPlayerName(), shootStats);
                        return shootStats;

                    }

                    @Override
                    public void close() {

                    }
                }, STATE_STORE_NAME)
                .toStream()
                //最好成绩大于8的才进行展示
                .filter((playerName, shootStats) -> shootStats.getBestScore() > 8)
                .filterNot((playerName, shootStats) -> "FINISHED".equals(shootStats.getStatus()))
                .print(Printed.<String, ShootStats>toSysOut().withLabel("ShootStats"));

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
