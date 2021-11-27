package com.github.udemy_kafka.kafka.tutorial_1.producer;

import com.github.udemy_kafka.kafka.tutorial_1.util.CallbackDemoImpl;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    public static Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    public static CallbackDemoImpl callback = new CallbackDemoImpl(logger);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        String topic = "first_topic";

        for (int i = 0; i < 10; i++) {
            // create producer record
            String value = "hello, world " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            logger.info("Key: {}", key);
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            // send data - asynchronous
            producer.send(record, callback).get(); // blocks the .send() to make it synchronous - bad practise
        }
        // flush data
        producer.flush();

        // close data
        producer.close();
    }
}
