package com.bigdata.it4931.layer.infrastructure.kafka.message;

public record KafkaMessage<T>(T value, String topic, long timestamp) {
    public KafkaMessage {
        if (value == null) {
            throw new IllegalArgumentException("value cannot be null");
        }
        if (topic == null) {
            throw new IllegalArgumentException("topic cannot be null");
        }
    }
}
