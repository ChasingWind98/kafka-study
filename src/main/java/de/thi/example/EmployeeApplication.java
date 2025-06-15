package de.thi.example;

import de.thi.example.model.Employee;
import de.thi.example.model.Sales;
import de.thi.example.serdes.JsonSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class EmployeeApplication {
    private static final Logger log = LoggerFactory.getLogger(EmployeeApplication.class);

    //0. 定义一些常量
    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String APPLICATION_ID = "employee_app_id";
    private static final String EMPLOYEE_SOURCE_TOPIC = "employee.topic";
    private static final String EMPLOYEE_TARGET_TOPIC = "employee.title.topic";


    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        //设置StateStore的位置 默认是 /tmp/kafka-streams
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "/Users/chasingwind/Kafka/statestore");
        //设置线程数 跟Partition数一致
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);


        StreamsBuilder builder = new StreamsBuilder();
        KTable<String, Employee> kt = builder.table(EMPLOYEE_SOURCE_TOPIC, Consumed.with(Serdes.String(), JsonSerdes.Employee())
                .withName("employee-tabel-processor")
                .withOffsetResetPolicy(AutoOffsetReset.earliest())
        );
        kt.filter((k, v) -> {
                    log.info("filter Department   ====> EmpNo:{}, Emp:{}", k, v);
                    return "IT".equals(v.getDepartment());
                })
                .filterNot((k, v) -> {
                    log.info("filter Age   ====> EmpNo:{}, Emp:{}", k, v);
                    return v.getAge() > 65;
                })
                .mapValues(employee -> {
                    log.info("mapValues   ====>  Emp:{}", employee);
                    return Employee.Builder.newBuilder(employee)
                            .evaluateTitle()
                            .build();
                })
                .toStream(Named.as("employee-print-processor"))
                .to(EMPLOYEE_TARGET_TOPIC, Produced.with(Serdes.String(), JsonSerdes.Employee()));

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
