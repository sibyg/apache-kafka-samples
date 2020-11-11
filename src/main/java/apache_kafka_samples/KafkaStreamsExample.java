package apache_kafka_samples;

import apache_kafka_samples.config.StreamConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Demonstrates group-by operations and aggregations on KTable. In this specific example we
 * compute the user count per geo-region from a KTable that contains {@code <user, region>} information.
 * <p>
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 * <p>
 * <br>
 * HOW TO RUN THIS EXAMPLE
 * <p>
 * 1) Start Zookeeper and Kafka. Please refer to <a href='https://kafka.apache.org/quickstart'>QuickStart</a>.
 * <p>
 * 2) Create the input and output topics used by this example.
 * <pre>
 * {@code
 * $ sh bin/kafka-topics.sh --create --topic usr-loc --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * $ sh bin/kafka-topics.sh --create --topic loc-stat --zookeeper localhost:2181 --partitions 1 --replication-factor 1
 * }</pre>
 * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be {@code bin/kafka-topics.sh ...}.
 * <p>
 * 3) Start this example application either in your IDE or on the command line.
 * <p>
 * If via the command line please refer to <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>.
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp target/kafka-streams-examples-5.5.0-standalone.jar io.confluent.examples.streams.UserRegionLambdaExample
 * }
 * </pre>
 * 4) Write some input data to the source topics (e.g. via {@code kafka-console-producer}). The already
 * running example application (step 3) will automatically process this input data and write the
 * results to the output topic.
 * <pre>
 * {@code
 * # Start the console producer, then input some example data records. The input data you enter
 * # should be in the form of USER,REGION<ENTER> and, because this example is set to discard any
 * # regions that have a user count of only 1, at least one region should have two users or more --
 * # otherwise this example won't produce any output data (cf. step 5).
 * #
 * # alice,edn<ENTER>
 * # bob,edn<ENTER>
 * # james,edn<ENTER>
 * # chao,lon<ENTER>
 * # dave,lon<ENTER>
 * # rob,cardiff<ENTER>
 * # alice,lon<ENTER>        <<< Note: Alice moved from dundee to london

 * #
 * # Here, the part before the comma will become the message key, and the part after the comma will
 * # become the message value.
 * $ bin/kafka-console-producer.sh --broker-list localhost:9092 --topic usr-loc --property parse.key=true --property key.separator=,
 * }</pre>
 * 5) Inspect the resulting data in the output topics, e.g. via {@code kafka-console-consumer}.
 * <pre>
 * {@code
 * $ bin/kafka-console-consumer.sh --topic loc-stat --from-beginning --bootstrap-server localhost:9092 --property print.key=true --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 * }</pre>
 * You should see output data similar to:
 * <pre>
 * {@code
 * edn  2     # because Bob and Eve are currently in edn
 * lon  3     # because Chao and Fang are currently in lon
 * }</pre>
 * 6) Once you're done with your experiments, you can stop this example via {@code Ctrl-C}. If needed,
 * also stop the Kafka broker ({@code Ctrl-C}), and only then stop the ZooKeeper instance ({@code Ctrl-C}).
 */
public class KafkaStreamsExample {

    public static void main(final String[] args) {

        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, String> lbgScotland = builder.table("usr-loc");

        // Aggregate the user counts of by region
        final KTable<String, Long> regionCounts = lbgScotland

                // Count by region;
                // no need to specify explicit serdes because the resulting key and value types match our default serde settings
                .groupBy((userId, region) -> KeyValue.pair(region, region))
                .count()
                // discard any regions with only 1 user
                .filter((regionName, count) -> count >= 2);

        // Note: The following operations would NOT be needed for the actual users-per-region
        // computation, which would normally stop at the filter() above.  We use the operations
        // below only to "massage" the output data so it is easier to inspect on the console via
        // kafka-console-consumer.
        //
        final KStream<String, Long> regionCountsForConsole = regionCounts
                // get rid of windows (and the underlying KTable) by transforming the KTable to a KStream
                .toStream()
                // sanitize the output by removing null record values (again, we do this only so that the
                // output is easier to read via kafka-console-consumer combined with LongDeserializer
                // because LongDeserializer fails on null values, and even though we could configure
                // kafka-console-consumer to skip messages on error the output still wouldn't look pretty)
                .filter((regionName, count) -> count != null);

        // write to the result topic, we need to override the value serializer to for type long
        regionCountsForConsole.to("loc-stat", Produced.with(stringSerde, longSerde));

        final KafkaStreams streams = new KafkaStreams(builder.build(), StreamConfig.streamConfig());

        streams.cleanUp();
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
