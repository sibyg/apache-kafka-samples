package apache_kafka_samples.custom;

import com.fasterxml.jackson.databind.ObjectMapper;
import apache_kafka_samples.pojo.ImmediatePayment;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ImmediatePaymentSerializer implements Serializer<ImmediatePayment> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, ImmediatePayment data) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsString(data).getBytes();
        } catch (Exception exception) {
            System.out.println("Error in serializing object" + data);
        }
        return retVal;
    }

    @Override
    public void close() {
    }
}