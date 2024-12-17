package com.bigdata.it4931.layer.infrastructure.kafka.message;

public class KafkaMessage<T> {
    private T value;
    private String topic;
    private long timestamp;

    public KafkaMessage(T value, String topic, long timestamp) {
        this.value = value;
        this.topic = topic;
        this.timestamp = timestamp;
    }

    public KafkaMessage() {
        if (value == null) {
            throw new IllegalArgumentException("value cannot be null");
        }
        if (topic == null) {
            throw new IllegalArgumentException("topic cannot be null");
        }
    }

    public T value() {
        return value;
    }

    public String topic() {
        return topic;
    }

    public long timestamp() {
        return timestamp;
    }
}
