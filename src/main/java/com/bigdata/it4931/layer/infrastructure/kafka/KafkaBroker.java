package com.bigdata.it4931.layer.infrastructure.kafka;

import java.util.Collection;
import java.util.Properties;

public class KafkaBroker {
    protected final Properties properties;
    protected final Collection<String> topics;

    protected KafkaBroker(Properties properties, Collection<String> topics) {
        this.properties = properties;
        this.topics = topics;
    }
}