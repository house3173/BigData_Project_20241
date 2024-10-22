package com.bigdata.it4931.layer.application.service.serving;

public interface IKafkaConsumerThread extends Runnable {
    void start();

    void stop();
}