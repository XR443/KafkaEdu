package com.github.raspopov.service;

import com.github.raspopov.data.Payload;
import com.github.raspopov.data.PayloadSerDe;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Locale;
import java.util.Properties;

public class KafkaStreamsService extends KafkaService {

    private KafkaStreams streams;

    public KafkaStreamsService() {
        super();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (isStreamCreated())
                streams.close();
        }));
    }

    public boolean isStreamCreated() {
        return streams != null;
    }

    public void createKafkaStream(String applicationId, String from, String to) {
        System.out.println("#########");
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        final KStream<String, Payload> stream = streamsBuilder.stream(from);

        stream
                .peek((key, value) -> System.out.println("#########"))
                .peek((key, value) -> System.out.printf("Consumed by Stream:%d - Kafka message data: %s%n", stream.toString().hashCode(), value.getData()))
                .peek((key, value) -> value.setData(value.getData().toUpperCase(Locale.ROOT)))
                .peek((key, value) -> System.out.printf("Updated by Stream%d - Kafka message data: %s%n", stream.toString().hashCode(), value.getData()))
                .peek((key, value) -> System.out.println("#########"))
                .to(to, Produced.with(Serdes.String(), new PayloadSerDe()));


        Properties props = new Properties();
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.putIfAbsent(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, PayloadSerDe.class);

        streams = new KafkaStreams(streamsBuilder.build(), props);
        streams.start();

        System.out.println("Stream " + streams.toString().hashCode() + " subscribed to " + from + " and write to " + to);
        System.out.println("#########");
    }
}
