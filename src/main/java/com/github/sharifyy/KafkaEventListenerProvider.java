package com.github.sharifyy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jboss.logging.Logger;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class KafkaEventListenerProvider implements EventListenerProvider {

    private static final Logger LOG = Logger.getLogger(KafkaEventListenerProvider.class);

    private final List<EventType> events;

    private final KafkaEventProducer kafkaEventProducer;
    private final KafkaEventConsumer kafkaEventConsumer;
    private final ObjectMapper mapper;

    public KafkaEventListenerProvider(KafkaEventProducer kafkaEventProducer, KafkaEventConsumer kafkaEventConsumer) {
        LOG.debug("starting listener provider");
        this.events = Arrays.stream(EventType.values())
                .collect(Collectors.toList());

        mapper = new ObjectMapper();
        this.kafkaEventProducer = kafkaEventProducer;
        this.kafkaEventConsumer = kafkaEventConsumer;
        // starting new thread for kafka consumer
        new Thread(this.kafkaEventConsumer).start();
    }


    @Override
    public void onEvent(Event event) {
        String topicEvents = "keycloakEvents";
        LOG.info("onEvent for user event on topic " + topicEvents);
        if (events.contains(event.getType())) {
            try {
                LOG.info("sending user event ");
                kafkaEventProducer.publishEvent(mapper.writeValueAsString(event), topicEvents);
            } catch (JsonProcessingException | ExecutionException | TimeoutException e) {
                LOG.error(e.getMessage(), e);
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void onEvent(AdminEvent event, boolean includeRepresentation) {
        String topicAdminEvents = "keycloakAdminEvents";
        LOG.info("onEvent for admin event on topic " + topicAdminEvents);
        try {
            LOG.info("sending admin event ");
            kafkaEventProducer.publishEvent(mapper.writeValueAsString(event), topicAdminEvents);
        } catch (JsonProcessingException | ExecutionException | TimeoutException e) {
            LOG.error(e.getMessage(), e);
        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        // ignore
    }

    public void shutDownConsumer() {
        LOG.info("shutting down the consumer");
        kafkaEventConsumer.shutDown();
    }
}