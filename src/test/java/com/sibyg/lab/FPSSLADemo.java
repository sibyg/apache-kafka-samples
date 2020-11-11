package com.sibyg.lab;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.sibyg.lab.TestUtils.tempDirectory;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * End-to-end integration test that demonstrates how to perform SLA using a left join between two KStreams.
 */
@Slf4j
public class FPSSLADemo {

    private static final String OUTBOUND_PAYMENT_REQUEST_TOPIC = "OUTBOUND_PAYMENT_REQUEST_TOPIC";
    private static final String OUTBOUND_PAYMENT_VALIDATION_SUCCESS_TOPIC = "OUTBOUND_PAYMENT_VALIDATION_SUCCESS_TOPIC";
    private static final String OUTBOUND_ACCOUNT_POSTING_SUCCESS_TOPIC = "OUTBOUND_ACCOUNT_POSTING_SUCCESS_TOPIC";
    private static final String OUTBOUND_PAYMENT_SEND_REQUEST_TOPIC = "OUTBOUND_PAYMENT_SEND_REQUEST_TOPIC";
    private static final String OUTBOUND_PAYMENT_DELIVERY_CONFIRMED_TOPIC = "OUTBOUND_PAYMENT_DELIVERY_CONFIRMED_TOPIC";
    private static final String SLA_TOPIC = "SLA_TOPIC";

    @Test
    public void shouldCorrelateKafkaStreams() {

        // Input 1: Payment Access Services Events
        final List<KeyValue<String, String>> pasEvents = Arrays.asList(
                new KeyValue<>("FPS_KEY_1", "OUTBOUND_PAYMENT_REQUEST_1"),
                new KeyValue<>("FPS_KEY_2", "OUTBOUND_PAYMENT_REQUEST_2"),
                new KeyValue<>("FPS_KEY_3", "OUTBOUND_PAYMENT_REQUEST_3"),
                new KeyValue<>("FPS_KEY_4", "OUTBOUND_PAYMENT_REQUEST_4"),
                new KeyValue<>("FPS_KEY_5", "OUTBOUND_PAYMENT_REQUEST_5")
        );

        // Input 2: FPS Agency Service Events
        final List<KeyValue<String, String>> fpsAgencyServiceEvents = Arrays.asList(
                new KeyValue<>("FPS_KEY_1", "OUTBOUND_PAYMENT_VALIDATION_SUCCESS_1"),
                new KeyValue<>("FPS_KEY_3", "OUTBOUND_PAYMENT_VALIDATION_SUCCESS_3"),
                new KeyValue<>("FPS_KEY_4", "OUTBOUND_PAYMENT_VALIDATION_SUCCESS_4"),
                new KeyValue<>("FPS_KEY_5", "OUTBOUND_PAYMENT_VALIDATION_SUCCESS_5")
        );

        // Input 3: Accounting Services Events
        final List<KeyValue<String, String>> accountingServicesEvents = Arrays.asList(
                new KeyValue<>("FPS_KEY_1", "OUTBOUND_ACCOUNT_POSTING_SUCCESS_1"),
                new KeyValue<>("FPS_KEY_4", "OUTBOUND_ACCOUNT_POSTING_SUCCESS_4"),
                new KeyValue<>("FPS_KEY_5", "OUTBOUND_ACCOUNT_POSTING_SUCCESS_5")
        );

        // Input 4: SAAS Gateway Events
        final List<KeyValue<String, String>> saasGwServiceEvents = Arrays.asList(
                new KeyValue<>("FPS_KEY_1", "OUTBOUND_PAYMENT_SEND_REQUEST_1"),
                new KeyValue<>("FPS_KEY_5", "OUTBOUND_PAYMENT_SEND_REQUEST_5")
        );

        // Input 5: Form3 Response Events
        final List<KeyValue<String, String>> form3ResponseEvents = Arrays.asList(
                new KeyValue<>("FPS_KEY_1", "OUTBOUND_PAYMENT_DELIVERY_CONFIRMED_1")
        );


        //
        // Step 1: Configure and start the processor topology.
        //
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-stream-join-lambda-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy config");
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Use a temporary directory for storing state, which will be automatically removed after the test.
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, tempDirectory().getAbsolutePath());

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> pasStreams = builder.stream(OUTBOUND_PAYMENT_REQUEST_TOPIC);
        final KStream<String, String> fpsAgencyServiceStreams = builder.stream(OUTBOUND_PAYMENT_VALIDATION_SUCCESS_TOPIC);
        final KStream<String, String> accountingServicesStreams = builder.stream(OUTBOUND_ACCOUNT_POSTING_SUCCESS_TOPIC);
        final KStream<String, String> saasGwServiceStreams = builder.stream(OUTBOUND_PAYMENT_SEND_REQUEST_TOPIC);
        final KStream<String, String> form3ResponseStreams = builder.stream(OUTBOUND_PAYMENT_DELIVERY_CONFIRMED_TOPIC);


        // In this example, we opt to perform an LEFT JOIN between the all streams.
        final KStream<String, String> fpsCorrelation = pasStreams
                .leftJoin(fpsAgencyServiceStreams, (leftValue, rightValue) ->
                                (rightValue == null) ?
                                        leftValue + "/NULL" :
                                        leftValue + "/" + rightValue
                        ,
                        // KStream-KStream joins are always windowed joins, hence we must provide a join window.
                        JoinWindows.of(Duration.ofSeconds(5)),
                        // In this specific example, we don't need to define join serdes explicitly because the key, left value, and
                        // right value are all of type String, which matches our default serdes configured for the application.  However,
                        // we want to showcase the use of `StreamJoined.with(...)` in case your code needs a different type setup.
                        StreamJoined.with(
                                Serdes.String(), /* key */
                                Serdes.String(), /* left value */
                                Serdes.String()  /* right value */
                        ))
                .leftJoin(accountingServicesStreams, (leftValue, rightValue) ->
                                (rightValue == null) ?
                                        leftValue + "/NULL" :
                                        leftValue + "/" + rightValue
                        ,
                        // KStream-KStream joins are always windowed joins, hence we must provide a join window.
                        JoinWindows.of(Duration.ofSeconds(5)),
                        // In this specific example, we don't need to define join serdes explicitly because the key, left value, and
                        // right value are all of type String, which matches our default serdes configured for the application.  However,
                        // we want to showcase the use of `StreamJoined.with(...)` in case your code needs a different type setup.
                        StreamJoined.with(
                                Serdes.String(), /* key */
                                Serdes.String(), /* left value */
                                Serdes.String()  /* right value */
                        ))
                .leftJoin(saasGwServiceStreams, (leftValue, rightValue) ->
                                (rightValue == null) ?
                                        leftValue + "/NULL" :
                                        leftValue + "/" + rightValue
                        ,
                        // KStream-KStream joins are always windowed joins, hence we must provide a join window.
                        JoinWindows.of(Duration.ofSeconds(5)),
                        // In this specific example, we don't need to define join serdes explicitly because the key, left value, and
                        // right value are all of type String, which matches our default serdes configured for the application.  However,
                        // we want to showcase the use of `StreamJoined.with(...)` in case your code needs a different type setup.
                        StreamJoined.with(
                                Serdes.String(), /* key */
                                Serdes.String(), /* left value */
                                Serdes.String()  /* right value */
                        ))
                .leftJoin(form3ResponseStreams, (leftValue, rightValue) ->
                                (rightValue == null) ?
                                        leftValue + "/NULL" :
                                        leftValue + "/" + rightValue
                        ,
                        // KStream-KStream joins are always windowed joins, hence we must provide a join window.
                        JoinWindows.of(Duration.ofSeconds(5)),
                        // In this specific example, we don't need to define join serdes explicitly because the key, left value, and
                        // right value are all of type String, which matches our default serdes configured for the application.  However,
                        // we want to showcase the use of `StreamJoined.with(...)` in case your code needs a different type setup.
                        StreamJoined.with(
                                Serdes.String(), /* key */
                                Serdes.String(), /* left value */
                                Serdes.String()  /* right value */
                        ));

        // Write the results to the output topic.
        fpsCorrelation.to(SLA_TOPIC);

        try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), streamsConfiguration)) {
            //
            // Step 2: Setup input and output topics.
            //
            final TestInputTopic<String, String> pasTestInputTopic = topologyTestDriver
                    .createInputTopic(OUTBOUND_PAYMENT_REQUEST_TOPIC,
                            new StringSerializer(),
                            new StringSerializer());
            final TestInputTopic<String, String> fpsAgencyServiceTestInputTopic = topologyTestDriver
                    .createInputTopic(OUTBOUND_PAYMENT_VALIDATION_SUCCESS_TOPIC,
                            new StringSerializer(),
                            new StringSerializer());
            final TestInputTopic<String, String> accountingServiceTestInputTopic = topologyTestDriver
                    .createInputTopic(OUTBOUND_ACCOUNT_POSTING_SUCCESS_TOPIC,
                            new StringSerializer(),
                            new StringSerializer());
            final TestInputTopic<String, String> saasGwServiceStreamsTestInputTopic = topologyTestDriver
                    .createInputTopic(OUTBOUND_PAYMENT_SEND_REQUEST_TOPIC,
                            new StringSerializer(),
                            new StringSerializer());
            final TestInputTopic<String, String> form3ResponseTestInputTopic = topologyTestDriver
                    .createInputTopic(OUTBOUND_PAYMENT_DELIVERY_CONFIRMED_TOPIC,
                            new StringSerializer(),
                            new StringSerializer());
            final TestOutputTopic<String, String> slaTestOutputTopic = topologyTestDriver
                    .createOutputTopic(SLA_TOPIC, new StringDeserializer(), new StringDeserializer());

            //
            // Step 3: Publish input data.
            //
            pasTestInputTopic.pipeKeyValueList(pasEvents);
            fpsAgencyServiceTestInputTopic.pipeKeyValueList(fpsAgencyServiceEvents);
            accountingServiceTestInputTopic.pipeKeyValueList(accountingServicesEvents);
            saasGwServiceStreamsTestInputTopic.pipeKeyValueList(saasGwServiceEvents);
            form3ResponseTestInputTopic.pipeKeyValueList(form3ResponseEvents);

            //
            // Step 4: Verify the application's output data.
            //
            Map<String, String> slaMap = slaTestOutputTopic.readKeyValuesToMap();

            slaMap.forEach((k, v) -> System.out.println("KEY:"+k+ " VALUE:"+v));

            assertThat(slaMap.get("FPS_KEY_1"), equalTo(
                    "OUTBOUND_PAYMENT_REQUEST_1/" +
                            "OUTBOUND_PAYMENT_VALIDATION_SUCCESS_1/" +
                            "OUTBOUND_ACCOUNT_POSTING_SUCCESS_1/" +
                            "OUTBOUND_PAYMENT_SEND_REQUEST_1/" +
                            "OUTBOUND_PAYMENT_DELIVERY_CONFIRMED_1"));
            assertThat(slaMap.get("FPS_KEY_2"), equalTo(
                    "OUTBOUND_PAYMENT_REQUEST_2/" +
                            "NULL/" +
                            "NULL/" +
                            "NULL/" +
                            "NULL"));
            assertThat(slaMap.get("FPS_KEY_3"), equalTo(
                    "OUTBOUND_PAYMENT_REQUEST_3/" +
                            "OUTBOUND_PAYMENT_VALIDATION_SUCCESS_3/" +
                            "NULL/" +
                            "NULL/" +
                            "NULL"));
            assertThat(slaMap.get("FPS_KEY_4"), equalTo(
                    "OUTBOUND_PAYMENT_REQUEST_4/" +
                            "OUTBOUND_PAYMENT_VALIDATION_SUCCESS_4/" +
                            "OUTBOUND_ACCOUNT_POSTING_SUCCESS_4/" +
                            "NULL/" +
                            "NULL"));
            assertThat(slaMap.get("FPS_KEY_5"), equalTo(
                    "OUTBOUND_PAYMENT_REQUEST_5/" +
                            "OUTBOUND_PAYMENT_VALIDATION_SUCCESS_5/" +
                            "OUTBOUND_ACCOUNT_POSTING_SUCCESS_5/" +
                            "OUTBOUND_PAYMENT_SEND_REQUEST_5/" +
                            "NULL"));
        }
    }
}