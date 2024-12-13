package com.bigdata.it4931;

import com.bigdata.it4931.layer.application.service.serving.IKafkaConsumerThread;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
public class Application implements CommandLineRunner {
    private final ExecutorService taskExecutor;
    private final List<IKafkaConsumerThread> kafkaConsumerThreads;

    public Application(List<IKafkaConsumerThread> kafkaConsumerThreads,
                       @Qualifier("cacheExecutorService") ExecutorService taskExecutor) {
        this.kafkaConsumerThreads = kafkaConsumerThreads;
        this.taskExecutor = taskExecutor;
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) {
        for (IKafkaConsumerThread kafkaConsumerThread : kafkaConsumerThreads) {
            taskExecutor.submit(kafkaConsumerThread);
        }
    }
}
