package org.dream.connector.pubsub;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.gcp.pubsub.common.PubSubDeserializationSchema;

import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Deserialization schema to deserialize messages produced by {@link PubSubPublisher}. The byte[]
 * received by this schema must contain a single Integer.
 */
class IntegerSerializer
        implements PubSubDeserializationSchema<Integer>, SerializationSchema<Integer> {

    @Override
    public Integer deserialize(PubsubMessage message) throws IOException {
        return new BigInteger(message.getData().toByteArray()).intValue();
    }

    @Override
    public boolean isEndOfStream(Integer integer) {
        return false;
    }

    @Override
    public TypeInformation<Integer> getProducedType() {
        return TypeInformation.of(Integer.class);
    }

    @Override
    public byte[] serialize(Integer integer) {
        return BigInteger.valueOf(integer).toByteArray();
    }
}