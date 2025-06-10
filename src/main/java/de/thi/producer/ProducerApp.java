package de.thi.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class ProducerApp {

    public static final String TOPIC = "my-topic";

    public static void main(String[] args) {
        produceMessages();
    }

    public static void produceMessages() {

        //Kafka配置
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        //序列化配置
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        //使用自定义Partitioner
//        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "de.thi.producer.SimplePartition");

        //Kafka Producer主对象
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        //Kafka 发送消息对象
        for (int i = 0; i < 1000; i++) {
            long timeMillis = System.currentTimeMillis();
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, "Key" + timeMillis, "Value " + timeMillis);
            //发送消息
            kafkaProducer.send(producerRecord, new Callback(){

                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    System.out.println("Message = " + producerRecord.key() + ", Partition = " + recordMetadata.partition());
                }
            });
        }

        //关闭资源
        kafkaProducer.close();
    }
}
