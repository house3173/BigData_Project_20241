package com.bigdata.it4931.config.kafka.properties;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KafkaConsumerProperties {
    private String bootstrapServers;
    private String enableAutoCommit;
    private String autoOffsetReset;
    private Integer maxPollRecords;
    private Integer maxPollIntervalMs;
    private boolean auth;
    private String username;
    private String password;
    private String groupId;
    private String keyDeserializer = StringDeserializer.class.getName();
    private String valueDeserializer = ByteArrayDeserializer.class.getName();
}

