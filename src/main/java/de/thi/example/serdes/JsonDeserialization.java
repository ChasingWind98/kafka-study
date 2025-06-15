package de.thi.example.serdes;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

import static de.thi.example.serdes.JsonSerialization.MAPPER;

class JsonDeserialization<T> implements Deserializer<T> {


    private Class<T> deserializedClass;

    public JsonDeserialization(Class<T> deserializedClass) {
        this.deserializedClass = deserializedClass;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            return MAPPER.readValue(data, deserializedClass);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
