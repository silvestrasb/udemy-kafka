package com.github.udemy_kafka.kafka.tutorial_1.util;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;

public class CallbackDemoImpl implements Callback {

    private Logger logger;

    public CallbackDemoImpl(Logger logger) {
        this.logger = logger;
    }

    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            // the record was successfully sent
            logger.info("-------- Received new metadata --------");
            logger.info("Topic: {}", recordMetadata.topic());
            logger.info("Partition: {}", recordMetadata.partition());
            logger.info("Offset: {}", recordMetadata.offset());
            logger.info("Timestamp: {}", recordMetadata.timestamp());
        } else {
            logger.error("Error while producing", e);
        }
    }
}
