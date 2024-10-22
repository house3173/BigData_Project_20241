package com.bigdata.it4931.layer.application.service.serving.impl;

import com.bigdata.it4931.layer.application.service.serving.IKafkaConsumerThread;
import com.bigdata.it4931.layer.infrastructure.kafka.read.KafkaBrokerReader;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Slf4j
public class KafkaConsumerThread implements IKafkaConsumerThread {
    private final List<KafkaBrokerReader> kafkaBrokerReaders;

    public KafkaConsumerThread(List<KafkaBrokerReader> kafkaBrokerReaders) {
        this.kafkaBrokerReaders = kafkaBrokerReaders;
    }

    @Override
    public void start() {
        log.info("Starting {} consumers", kafkaBrokerReaders.size());
        for (KafkaBrokerReader kafkaBrokerReader : kafkaBrokerReaders) {
            kafkaBrokerReader.start();
        }
        log.info("Started {} consumers", kafkaBrokerReaders.size());
    }

    @Override
    public void stop() {
        log.info("Stopping {} consumers", kafkaBrokerReaders.size());
        for (KafkaBrokerReader kafkaBrokerReader : kafkaBrokerReaders) {
            kafkaBrokerReader.stop();
        }
        log.info("Stopped {} consumers", kafkaBrokerReaders.size());
    }

    @Override
    public void run() {
        try {
            start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
