package com.github.raspopov.data;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.util.Map;

public class PayloadSerDe implements Serializer<Payload>, Deserializer<Payload>, Serde<Payload> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @SneakyThrows
    @Override
    public Payload deserialize(String topic, byte[] data) {
        return objectMapper.readValue(data, Payload.class);
    }

    @SneakyThrows
    @Override
    public byte[] serialize(String topic, Payload data) {
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        objectMapper.writeValue(byteOutputStream, data);
        return byteOutputStream.toByteArray();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Serializer.super.configure(configs, isKey);
    }

    @Override
    public void close() {
        Serializer.super.close();
    }

    @Override
    public Serializer<Payload> serializer() {
        return this;
    }

    @Override
    public Deserializer<Payload> deserializer() {
        return this;
    }
}
