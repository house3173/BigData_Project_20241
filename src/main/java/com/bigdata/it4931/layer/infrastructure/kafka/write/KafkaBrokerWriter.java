package com.bigdata.it4931.layer.infrastructure.kafka.write;

import com.bigdata.it4931.layer.infrastructure.kafka.KafkaBroker;
import com.bigdata.it4931.utility.kafka.KafkaUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaBrokerWriter extends KafkaBroker {
    private final Producer<String, String> producer;

    public KafkaBrokerWriter(Properties properties, Collection<String> topics) {
        super(properties, topics);
        this.producer = KafkaUtils.initProducer(properties);
    }

    public List<Future<RecordMetadata>> write(String message) {
        List<Future<RecordMetadata>> futures = new ArrayList<>();
        for (String topic : topics) {
            futures.add(producer.send(new ProducerRecord<>(topic, message)));
        }
        return futures;
    }
}
