package com.bigdata.it4931.layer.application.service.serving.impl;

import com.bigdata.it4931.layer.infrastructure.kafka.write.KafkaBrokerWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Properties;

@Service
public class TestProducer {
    private final KafkaBrokerWriter kafkaBrokerWriter;

    public TestProducer(@Qualifier("kafkaBrokerWriterProperties") Properties properties,
                        @Value("${kafka.producer.topic}") String topic) {
        this.kafkaBrokerWriter = new KafkaBrokerWriter(properties, Collections.singletonList(topic));
    }

    @Scheduled(fixedRate = 2000)
    public void send() {
        kafkaBrokerWriter.write(System.currentTimeMillis() + "");
    }
}
