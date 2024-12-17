package com.bigdata.it4931.layer.application.service.batch;

import com.bigdata.it4931.config.Constants;
import com.bigdata.it4931.layer.application.domain.dto.JobDataDto;
import com.bigdata.it4931.layer.application.service.serving.ConsumeRunner;
import com.bigdata.it4931.layer.infrastructure.kafka.message.KafkaMessage;
import com.bigdata.it4931.layer.infrastructure.kafka.read.KafkaBrokerReader;
import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Service
@Slf4j
public class BatchConsumer extends KafkaBrokerReader implements ConsumeRunner {
    private final HdfsParquetService hdfsParquetService;

    public BatchConsumer(@Qualifier("kafkaBrokerBatchReaderProperties") Properties props,
                         @Value("${kafka.consumer.topic}") String topic,
                         HdfsParquetService hdfsParquetService) {
        super(props, Collections.singletonList(topic), 1, 10, 5);
        this.hdfsParquetService = hdfsParquetService;
    }

    @Override
    public void processing(List<KafkaMessage<String>> messages) {
        List<JobDataDto> jobDataList = new ArrayList<>();
        for (KafkaMessage<String> message : messages) {
            try {
                JobDataDto jobData = Constants.OBJECT_MAPPER.readValue(message.value(), JobDataDto.class);
                jobDataList.add(jobData);
            } catch (JsonProcessingException e) {
                log.error("Error when parse job data {}", e.getMessage(), e);
            }
        }
        hdfsParquetService.save(jobDataList);
    }

    @Override
    public void start(){
        log.info("Starting batch-layer consumer");
        super.start();
    }
}
