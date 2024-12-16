package com.bigdata.it4931;

import com.bigdata.it4931.layer.application.service.serving.ConsumeRunner;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.List;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableScheduling
@Slf4j
public class Application implements CommandLineRunner {
    private final List<ConsumeRunner> kafkaConsumerThreads;

    public Application(List<ConsumeRunner> kafkaConsumerThreads) {
        this.kafkaConsumerThreads = kafkaConsumerThreads;
    }

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Override
    public void run(String... args) {
        log.info("Starting consumers...");
        kafkaConsumerThreads.forEach(ConsumeRunner::start);
    }

    @PreDestroy
    protected void stopService() {
        kafkaConsumerThreads.forEach(ConsumeRunner::stop);
    }
}
