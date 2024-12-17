package com.bigdata.it4931.layer.infrastructure.kafka.read;

import com.bigdata.it4931.layer.infrastructure.kafka.KafkaBroker;
import com.bigdata.it4931.layer.infrastructure.kafka.message.KafkaMessage;
import com.bigdata.it4931.utility.kafka.KafkaUtils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public abstract class KafkaBrokerReader extends KafkaBroker {
    private final ExecutorService executor;
    private final int numConsumers;
    private final int minRecords;
    private final int maxWaitSeconds;
    private Long previousTime;
    private final AtomicBoolean running = new AtomicBoolean(false);

    protected KafkaBrokerReader(Properties consumerProperties,
                                Collection<String> topics,
                                Integer numConsumers,
                                Integer minRecords,
                                Integer maxWaitSeconds) {
        super(consumerProperties, topics);
        this.minRecords = (minRecords != null && minRecords > 0) ? minRecords : 1;
        this.numConsumers = (numConsumers != null && numConsumers > 0) ? numConsumers : Math.max(Runtime.getRuntime().availableProcessors() - 1, 1);
        this.maxWaitSeconds = (maxWaitSeconds != null && maxWaitSeconds > 0) ? maxWaitSeconds : 1;
        this.executor = Executors.newFixedThreadPool(this.numConsumers, new ThreadFactoryBuilder().setNameFormat(getClass().getSimpleName() + "-consumer-%d").build());

        // Setup shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
    }

    private void startKafkaConsumer() {
        if (topics.isEmpty()) {
            log.warn("No topics to subscribe to");
            stop();
        }

        while (running.get()) {
            try (Consumer<String, String> consumer = KafkaUtils.initConsumer(properties, topics)) {
                List<KafkaMessage<String>> messages = new ArrayList<>();
                while (running.get()) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    if (records.isEmpty()) {
                        continue;
                    }

                    for (ConsumerRecord<String, String> record : records) {
                        messages.add(new KafkaMessage<>(record.value(), record.topic(), record.timestamp()));
                    }

                    if (messages.size() > minRecords || waitingTimeExpired()) {
                        processing(messages);
                        consumer.commitSync();
                        messages.clear();
                        previousTime = System.currentTimeMillis();
                    }
                }
            } catch (Exception e) {
                log.error("Error while consuming messages", e);
            }
        }
        log.info("Consumer stopped at thread: {}", Thread.currentThread().getName());
    }

    public abstract void processing(List<KafkaMessage<String>> messages);

    private boolean waitingTimeExpired() {
        synchronized (KafkaBrokerReader.class) {
            return System.currentTimeMillis() - previousTime >= maxWaitSeconds * 1000L;
        }
    }

    public void start() {
        running.set(true);
        log.info("Starting {} consumer(s)", numConsumers);
        for (int i = 0; i < numConsumers; i++) {
            executor.submit(this::startKafkaConsumer);
        }
        previousTime = System.currentTimeMillis();
    }

    public void stop() {
        running.set(false);
        log.info("Shutting down Kafka consumer");
        executor.shutdown();
    }
}
