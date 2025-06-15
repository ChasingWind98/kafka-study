package de.thi.ktable;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimulationNullValueProducer {
    private static final Logger log = LoggerFactory.getLogger(SimulationNullValueProducer.class);

    //0. 定义一些常量
    private static final String BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String APPLICATION_ID = "null_value_app_id";
    private static final String SOURCE_TOPIC = "users.info";


    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer =  new KafkaProducer<>(properties);
        Future<RecordMetadata> send = kafkaProducer.send(new ProducerRecord<>(SOURCE_TOPIC,
                "001", null));
        RecordMetadata recordMetadata = send.get();
        log.info("Send message to topic: {}, partition: {}, offset: {}",
                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());

        kafkaProducer.close();

    }
}
