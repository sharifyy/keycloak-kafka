package com.github.sharifyy;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jboss.logging.Logger;

public final class KafkaEventProducer {

	private static final Logger LOG = Logger.getLogger(KafkaEventProducer.class);

	private final Producer<String, String> producer;

	public KafkaEventProducer(String bootstrapServer,
							  Map<String, Object> optionalProperties) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.putAll(optionalProperties);
		this.producer = new KafkaProducer<>(props);
	}


	public void publishEvent(String eventAsString, String topic)
			throws InterruptedException, ExecutionException, TimeoutException {
		LOG.debug("Produce to topic: " + topic + " ...");
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, eventAsString);
		producer.send(record, (recordMetadata, e) -> {
					if (e == null) {
						LOG.debug("Produced to topic: " + recordMetadata.topic());
					}else {
						LOG.error("failed to send message ",e);
					}
				}
		);
	}

}