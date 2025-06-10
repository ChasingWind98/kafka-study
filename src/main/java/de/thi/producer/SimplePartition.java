package de.thi.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义Partition  Angepasste Partitioner
 */
public class SimplePartition implements Partitioner {

    /**
     * 根据指定的键、值和集群信息来确定消息应该被发送到的主题中的哪个分区
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 以键为key1 key2 key3为例
        String keyStr = key + "";
        if (keyStr.contains("1") || keyStr.contains("2")) {
            return 1;
        } else
            return 0;
    }


    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
