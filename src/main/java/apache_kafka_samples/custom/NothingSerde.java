package apache_kafka_samples.custom;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Arrays;
import java.util.Map;

/**
 * A Serializer/Deserializer/Serde implementation for use when you know the data is always null
 *
 * @param <T> The type of the stream (you can parameterize this with any type,
 *            since we throw an exception if you attempt to use it with non-null data)
 */
public class NothingSerde<T> implements Serializer<T>, Deserializer<T>, Serde<T> {

    @Override
    public void configure(final Map<String, ?> configuration, final boolean isKey) {
    }

    @Override
    public T deserialize(final String topic, final byte[] bytes) {
        if (bytes != null) {
            throw new IllegalArgumentException("Expected [" + Arrays.toString(bytes) + "] to be null.");
        } else {
            return null;
        }
    }

    @Override
    public byte[] serialize(final String topic, final T data) {
        if (data != null) {
            throw new IllegalArgumentException("Expected [" + data + "] to be null.");
        } else {
            return null;
        }
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }
}
