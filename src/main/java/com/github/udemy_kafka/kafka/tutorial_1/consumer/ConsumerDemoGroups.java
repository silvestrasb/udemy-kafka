package com.github.udemy_kafka.kafka.tutorial_1.consumer;

import com.github.udemy_kafka.kafka.tutorial_1.util.CallbackDemoImpl;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroups {

    public static String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    public static Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);

    public static CallbackDemoImpl callback = new CallbackDemoImpl(logger);

    public static void main(String[] args) {
        String groupId = "my-first-app";
        String topic = "first_topic";

        // creates consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        // subscribe consumer to topic
        consumer.subscribe(Arrays.asList(topic));

        // poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("--------------- MESSAGE ---------------");
                logger.info("Key: {}", record.key());
                logger.info("Value: {}", record.value());
                logger.info("Partition: {}", record.partition());
                logger.info("Offset: {}", record.offset());
            }
        }

    }
}
