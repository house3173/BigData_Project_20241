package com.bigdata.it4931.config.kafka.properties;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaProducerProperties {
    private String bootstrapServers;
    private String acks;
    private Integer retries;
    private Integer batchSize;
    private Integer bufferMemory;
    private Integer requestTimeoutMs;
    private Integer lingerMs;
    private Integer deliveryTimeoutMs;
    private Integer retryBackoffMs;
    private boolean auth;
    private String username;
    private String password;
    private String keySerializer = StringSerializer.class.getName();
    private String valueSerializer = ByteArraySerializer.class.getName();
}

