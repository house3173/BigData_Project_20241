package com.bigdata.it4931.layer.application.service.serving.impl;

import com.bigdata.it4931.layer.infrastructure.kafka.message.KafkaMessage;
import com.bigdata.it4931.layer.infrastructure.kafka.read.KafkaBrokerReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Component
@Slf4j
public class TestConsumer extends KafkaBrokerReader {
    public TestConsumer(@Qualifier("kafkaBrokerReaderProperties") Properties props,
                        @Value("${kafka.consumer.topic}") String topic) {
        super(props, Collections.singletonList(topic), 2, 10, 5);
    }

    @Override
    public void processing(List<KafkaMessage<byte[]>> messages) {
        for (KafkaMessage<byte[]> message : messages) {
            log.info("Received message: {}", new String(message.value(), StandardCharsets.UTF_8));
        }
    }
}

