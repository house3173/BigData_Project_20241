package com.bigdata.it4931.config.kafka;

import com.bigdata.it4931.config.kafka.properties.KafkaConsumerProperties;
import com.bigdata.it4931.config.kafka.properties.KafkaProducerProperties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
public class KafkaConfiguration {

    @Bean("kafkaConsumerProperties")
    @ConfigurationProperties(prefix = "kafka.consumer")
    public KafkaConsumerProperties kafkaConsumerProperties() {
        return new KafkaConsumerProperties();
    }

    @Bean("kafkaBrokerReaderProperties")
    public Properties kafkaBrokerReaderProperties(@Qualifier("kafkaConsumerProperties") KafkaConsumerProperties kafkaConsumerProperties,
                                                  @Value("${kafka.consumer.group-id}") String groupId) {
        return getProperties(kafkaConsumerProperties, groupId);
    }

    @Bean("kafkaProducerProperties")
    @ConfigurationProperties(prefix = "kafka.producer")
    public KafkaProducerProperties kafkaProducerProperties() {
        return new KafkaProducerProperties();
    }

    @Bean("kafkaBrokerWriterProperties")
    public Properties kafkaBrokerWriterProperties(@Qualifier("kafkaProducerProperties") KafkaProducerProperties kafkaProducerProperties) {
        return getProperties(kafkaProducerProperties);
    }

    private Properties getProperties(KafkaConsumerProperties kafkaConsumerProperties,
                                     String groupId) {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConsumerProperties.getBootstrapServers());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, kafkaConsumerProperties.getEnableAutoCommit());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConsumerProperties.getAutoOffsetReset());
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, kafkaConsumerProperties.getMaxPollRecords());
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafkaConsumerProperties.getMaxPollIntervalMs());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaConsumerProperties.getKeyDeserializer());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaConsumerProperties.getValueDeserializer());

        // Kafka authentication config
        if (kafkaConsumerProperties.isAuth()) {
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
            properties.put(SaslConfigs.SASL_JAAS_CONFIG,
                    String.format("org.apache.kafka.common.security.scram.ScramLoginModule required username='%s' password='%s';",
                            kafkaConsumerProperties.getUsername(), kafkaConsumerProperties.getPassword())
            );
        }
        return properties;
    }

    private Properties getProperties(KafkaProducerProperties kafkaProducerProperties) {
        final Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProducerProperties.getBootstrapServers());
        properties.put(ProducerConfig.ACKS_CONFIG, kafkaProducerProperties.getAcks());
        properties.put(ProducerConfig.RETRIES_CONFIG, kafkaProducerProperties.getRetries());
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaProducerProperties.getBatchSize());
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, kafkaProducerProperties.getBufferMemory());
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaProducerProperties.getRequestTimeoutMs());
        properties.put(ProducerConfig.LINGER_MS_CONFIG, kafkaProducerProperties.getLingerMs());
        properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, kafkaProducerProperties.getDeliveryTimeoutMs());
        properties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, kafkaProducerProperties.getRetryBackoffMs());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProducerProperties.getKeySerializer());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProducerProperties.getValueSerializer());

        // Kafka authentication config
        if (kafkaProducerProperties.isAuth()) {
            properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
            properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
            properties.put(SaslConfigs.SASL_JAAS_CONFIG,
                    String.format("org.apache.kafka.common.security.scram.ScramLoginModule required username='%s' password='%s';",
                            kafkaProducerProperties.getUsername(), kafkaProducerProperties.getPassword())
            );
        }

        return properties;
    }
}
