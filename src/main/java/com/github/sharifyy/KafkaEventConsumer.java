package com.github.sharifyy;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaEventConsumer implements Runnable {

    private static final Logger LOG = Logger.getLogger(KafkaEventConsumer.class);

    private final Consumer<String, String> consumer;
    private final List<String> topics;


    public KafkaEventConsumer(String bootstrapServer,
                              Map<String, Object> optionalProperties,
                              List<String> topics) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "auth-server");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.putAll(optionalProperties);
        this.topics = topics;
        this.consumer = new KafkaConsumer<>(props);
    }


    @Override
    public void run() {
        consumer.subscribe(topics);
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    // do logic based on record.topic()
                    LOG.info("message received: " + record.value());
                }
            }
        } catch (WakeupException e) {
            LOG.info("shut down signal received");
        } finally {
            LOG.info("consumer closed finally");
            consumer.close();
        }
    }

    public void shutDown() {
        // to interrupt consumer.poll() throws WakeUpException
        consumer.wakeup();
    }
}
