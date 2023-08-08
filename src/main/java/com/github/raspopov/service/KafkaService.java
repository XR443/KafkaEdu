package com.github.raspopov.service;

import com.github.raspopov.data.Payload;
import com.github.raspopov.data.PayloadSerDe;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;

public class KafkaService {

    private KafkaProducer<String, Payload> producer;
    private KafkaConsumer<String, Payload> consumer;

    public KafkaService() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (isConsumerCreated())
                consumer.close();
            if (isProducerCreated())
                producer.close();
        }));
    }

    public boolean isProducerCreated() {
        return producer != null;
    }

    public boolean isConsumerCreated() {
        return consumer != null;
    }

    public void createKafkaProducer() {
        System.out.println("#########");
        if (producer != null) {
            producer.close();
        }

        Properties producerProperties = new Properties();
        producerProperties.put("key.serializer", StringSerializer.class);
        producerProperties.put("value.serializer", PayloadSerDe.class);
        producerProperties.put("bootstrap.servers", "localhost:9092");

        producer = new KafkaProducer<>(producerProperties);
        System.out.println("Created producer: " + producer.toString().hashCode());
        System.out.println("#########");
    }

    public void createKafkaConsumer() {
        createKafkaConsumer("consumer");
    }

    public void createKafkaConsumer(String groupId) {
        System.out.println("#########");
        if (consumer != null) {
            consumer.close();
        }

        Properties consumerProperties = new Properties();
        consumerProperties.put("key.deserializer", StringDeserializer.class);
        consumerProperties.put("value.deserializer", PayloadSerDe.class);
        consumerProperties.put("group.id", groupId);
        consumerProperties.put("bootstrap.servers", "localhost:9092");
        consumerProperties.put("auto.offset.reset", "earliest");
        consumerProperties.put("enable.auto.commit", "false");

        consumer = new KafkaConsumer<>(consumerProperties);
        System.out.println("Created consumer: " + consumer.toString().hashCode());
        System.out.println("#########");
    }

    public void subscribeKafkaConsumer(String topic) {
        System.out.println("#########");
        if (topic == null || topic.isEmpty()) {
            System.out.println("Set topic name before creating consumer");
            return;
        }
        consumer.subscribe(Collections.singletonList(topic));
        System.out.println("Consumer " + consumer.toString().hashCode() + " subscribed to " + topic);
        System.out.println("#########");
    }

    public void sendData(String topic, String data, int count) {
        System.out.println("#########");
        IntStream.range(0, count).forEach(value -> {
            try {
                producer.send(new ProducerRecord<>(topic, new Payload(data + " Number " + value)), (metadata, exception) -> {
                    if (exception != null)
                        exception.printStackTrace();
                });
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
        System.out.printf("Sent %s events to %s%n", count, topic);
        System.out.println("#########");
    }

    public void consumeRecords(boolean commit, long offset) {
        if (offset != 0) {
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
            for (Map.Entry<TopicPartition, Long> endOffset : endOffsets.entrySet()) {
                consumer.seek(endOffset.getKey(), endOffset.getValue() + offset);
            }
        }
        consumeRecords(commit);
    }

    public void consumeRecords(boolean commit) {
        System.out.println("#########");
        if (consumer == null) {
            System.out.println("Create Consumer first");
            return;
        } else {
            ConsumerRecords<String, Payload> poll = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Payload> record : poll) {
                Payload value = record.value();
                System.out.println("Consumed by Consumer:" + consumer.toString().hashCode() + " - Kafka message data: " + value.getData());
            }
            if (poll.isEmpty())
                System.out.println("No records consumed");
            if (commit)
                consumer.commitSync();
        }
        System.out.println("#########");
    }
}
