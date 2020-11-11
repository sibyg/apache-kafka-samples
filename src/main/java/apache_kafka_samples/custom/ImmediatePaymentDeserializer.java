package apache_kafka_samples.custom;

import com.fasterxml.jackson.databind.ObjectMapper;
import apache_kafka_samples.pojo.ImmediatePayment;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class ImmediatePaymentDeserializer implements Deserializer<ImmediatePayment> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public ImmediatePayment deserialize(String topic, byte[] data) {
        ObjectMapper mapper = new ObjectMapper();
        ImmediatePayment object = null;
        try {
            object = mapper.readValue(data, ImmediatePayment.class);
        } catch (Exception exception) {
            System.out.println("Error in deserializing bytes " + exception);
        }
        return object;
    }

    @Override
    public void close() {
    }
}