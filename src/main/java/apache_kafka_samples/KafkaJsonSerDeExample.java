package apache_kafka_samples;

import apache_kafka_samples.config.IKafkaConstants;
import apache_kafka_samples.factory.ConsumerFactory;
import apache_kafka_samples.factory.ProducerFactory;
import apache_kafka_samples.pojo.Account;
import apache_kafka_samples.pojo.ImmediatePayment;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.ExecutionException;

public class KafkaJsonSerDeExample {

    public static void main(String[] args) {
        runProducer();
        runConsumer();
    }

    static void runProducer() {
        final Long KEY = 10L;
        Producer<Long, ImmediatePayment> producer = ProducerFactory.createImmediatePaymentProducer();
        ImmediatePayment immediatePayment = createImmediatePayment();

        ProducerRecord<Long, ImmediatePayment> record = new ProducerRecord<Long, ImmediatePayment>(IKafkaConstants.TOPIC_NAME,
                KEY, immediatePayment);
        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.println("Record sent with key " + KEY + " to partition " + metadata.partition()
                    + " with offset " + metadata.offset());
        } catch (ExecutionException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        } catch (InterruptedException e) {
            System.out.println("Error in sending record");
            System.out.println(e);
        }
    }

    private static ImmediatePayment createImmediatePayment() {
        Account sender = new Account("402326", "23435434", 100, "GBP");
        Account debtor = new Account("402327", "23435435", 100, "GBP");
        return new ImmediatePayment(10, sender, debtor);
    }

    static void runConsumer() {
        Consumer<Long, String> consumer = ConsumerFactory.createConsumer();
        int noMessageFound = 0;
        while (true) {
            ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    // If no message found count is reached to threshold exit loop.
                    break;
                else
                    continue;
            }
            //print each record.
            consumerRecords.forEach(record -> {
                System.out.format("%nRecord Key: %s, Record value:%s, Record partition:%s, Record offset:%s",
                        record.key(), record.value(), record.partition(), record.offset());
            });
            // commits the offset of record to broker.
            consumer.commitAsync();
        }
        consumer.close();
    }
}