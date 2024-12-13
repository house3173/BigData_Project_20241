package com.bigdata.it4931.config;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class ExecutorServiceConfig {
    @Bean(value = "cacheExecutorService")
    public ExecutorService taskExecutor() {
        return Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("cache-thread-pool-%d").build());
    }
}
