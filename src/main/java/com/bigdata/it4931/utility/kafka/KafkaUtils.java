package com.bigdata.it4931.utility.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class KafkaUtils {
    public static <K, V> Consumer<K, V> initConsumer(Properties props, String... topicNames) {
        Consumer<K, V> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(new ArrayList<>(Arrays.asList(topicNames)));
        return consumer;
    }

    public static <K, V> Consumer<K, V> initConsumer(Properties props, Collection<String> topics) {
        Consumer<K, V> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(topics);
        return consumer;
    }

    public static <K, V> Producer<K, V> initProducer(Properties props) {
        return new KafkaProducer<>(props);
    }
}
