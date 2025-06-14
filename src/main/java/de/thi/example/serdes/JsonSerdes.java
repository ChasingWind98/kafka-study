package de.thi.example.serdes;

import de.thi.example.model.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerdes {

    public static NetTrafficWrapSerde NetTraffic() {
        return new NetTrafficWrapSerde(new JsonSerialization<>(), new JsonDeserialization<>(NetTraffic.class));
    }

    public final static class NetTrafficWrapSerde extends WrapSerde<NetTraffic> {
        public NetTrafficWrapSerde(Serializer<NetTraffic> serializer, Deserializer<NetTraffic> deserializer) {
            super(serializer, deserializer);
        }
    }

    public static SalesStatsWrapSerde SalesStats() {
        return new SalesStatsWrapSerde(new JsonSerialization<>(), new JsonDeserialization<>(SalesStats.class));
    }

    public final static class SalesStatsWrapSerde extends WrapSerde<SalesStats> {
        public SalesStatsWrapSerde(Serializer<SalesStats> serializer, Deserializer<SalesStats> deserializer) {
            super(serializer, deserializer);
        }
    }

    public static SalesWrapSerde Sales() {
        return new SalesWrapSerde(new JsonSerialization<>(), new JsonDeserialization<>(Sales.class));
    }

    public final static class SalesWrapSerde extends WrapSerde<Sales> {
        public SalesWrapSerde(Serializer<Sales> serializer, Deserializer<Sales> deserializer) {
            super(serializer, deserializer);
        }
    }


    public static TransactionWrapSerde Transaction() {
        return new TransactionWrapSerde(new JsonSerialization<>(), new JsonDeserialization<>(Transaction.class));
    }

    public final static class TransactionWrapSerde extends WrapSerde<Transaction> {
        public TransactionWrapSerde(Serializer<Transaction> serializer, Deserializer<Transaction> deserializer) {
            super(serializer, deserializer);
        }
    }


    public static TransactionPatternWrapSerde TransactionPattern() {
        return new TransactionPatternWrapSerde(new JsonSerialization<>(), new JsonDeserialization<>(TransactionPattern.class));
    }

    public final static class TransactionPatternWrapSerde extends WrapSerde<TransactionPattern> {
        public TransactionPatternWrapSerde(Serializer<TransactionPattern> serializer, Deserializer<TransactionPattern> deserializer) {
            super(serializer, deserializer);
        }
    }


    public static TransactionKeyWrapSerde TransactionKey() {
        return new TransactionKeyWrapSerde(new JsonSerialization<>(), new JsonDeserialization<>(TransactionKey.class));
    }

    public final static class TransactionKeyWrapSerde extends WrapSerde<TransactionKey> {
        public TransactionKeyWrapSerde(Serializer<TransactionKey> serializer, Deserializer<TransactionKey> deserializer) {
            super(serializer, deserializer);
        }
    }


    public static TransactionRewardWrapSerde TransactionReward() {
        return new TransactionRewardWrapSerde(new JsonSerialization<>(), new JsonDeserialization<>(TransactionReward.class));
    }

    public final static class TransactionRewardWrapSerde extends WrapSerde<TransactionReward> {
        public TransactionRewardWrapSerde(Serializer<TransactionReward> serializer, Deserializer<TransactionReward> deserializer) {
            super(serializer, deserializer);
        }
    }


    private static class WrapSerde<T> implements Serde<T> {

        private final Serializer<T> serializer;
        private final Deserializer<T> deserializer;

        public WrapSerde(Serializer<T> serializer, Deserializer<T> deserializer) {
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        @Override
        public Serializer<T> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<T> deserializer() {
            return deserializer;
        }
    }
}
