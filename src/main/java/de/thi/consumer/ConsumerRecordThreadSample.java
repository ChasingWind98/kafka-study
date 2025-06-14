package de.thi.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConsumerRecordThreadSample {

    public static final String MY_TOPIC = "my-topic";

    public static void main(String[] args) throws InterruptedException {
        String brokerList = "localhost:19092";
        String groupId = "test";
        int workerNum = 5;

        CunsumerExecutor consumers = new CunsumerExecutor(brokerList, groupId, MY_TOPIC);
        consumers.execute(workerNum);

        Thread.sleep(1000000);

        consumers.shutdown();

    }

    // Consumer处理
    public static class CunsumerExecutor {
        private final KafkaConsumer<String, String> consumer;
        private ExecutorService executors;

        public CunsumerExecutor(String brokerList, String groupId, String topic) {
            Properties props = new Properties();
            props.put("bootstrap.servers", brokerList);
            props.put("group.id", groupId);
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("session.timeout.ms", "30000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topic));
        }

        public void execute(int workerNum) {
            //创建线程池
            executors = new ThreadPoolExecutor(workerNum, workerNum, 0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (final ConsumerRecord record : records) {
                    //提交任务 每一个消息都是一条任务
                    executors.submit(new ConsumerRecordWorker(record));
                }
            }
        }

        public void shutdown() {
            if (consumer != null) {
                consumer.close();
            }
            if (executors != null) {
                executors.shutdown();
            }
            try {
                assert executors != null;
                if (!executors.awaitTermination(10, TimeUnit.SECONDS)) {
                    System.out.println("Timeout.... Ignore for this case");
                }
            } catch (InterruptedException ignored) {
                System.out.println("Other thread interrupted this shutdown, ignore for this case.");
                Thread.currentThread().interrupt();
            }
        }


    }

    // 处理每一条消息
    public static class ConsumerRecordWorker implements Runnable {

        private ConsumerRecord<String, String> record;

        public ConsumerRecordWorker(ConsumerRecord record) {
            this.record = record;
        }

        @Override
        public void run() {
            System.out.println("Thread - " + Thread.currentThread().getName());
            System.err.printf("patition = %d , offset = %d, key = %s, value = %s%n",
                    record.partition(), record.offset(), record.key(), record.value());
        }

    }
}
