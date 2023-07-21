package com.github.sharifyy;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.jboss.logging.Logger;
import org.keycloak.Config.Scope;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.events.EventType;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

public class KafkaEventListenerProviderFactory implements EventListenerProviderFactory {

    private static final Logger LOG = Logger.getLogger(KafkaEventListenerProviderFactory.class);
    private static final String ID = "kafka";

    private KafkaEventListenerProvider instance;

    private String bootstrapServers;
    private String[] events;
    private Map<String, Object> kafkaProperties;

    @Override
    public EventListenerProvider create(KeycloakSession session) {
        LOG.debug("starting listener factory");
        if (instance == null) {
            instance = new KafkaEventListenerProvider(
                    new KafkaEventProducer(bootstrapServers, kafkaProperties),
                    new KafkaEventConsumer(bootstrapServers, kafkaProperties, List.of("policy-enforcer")));
        }

        return instance;
    }

    @Override
    public String getId() {
        return ID;
    }

    @Override
    public void init(Scope config) {
        LOG.info("Init kafka module ...");
        bootstrapServers = config.get("bootstrapServers", System.getenv("KAFKA_BOOTSTRAP_SERVERS"));
        String eventsString = config.get("events", System.getenv("KAFKA_EVENTS"));

        if (eventsString != null) {
            events = eventsString.split(",");
        }

        if (bootstrapServers == null) {
            throw new NullPointerException("bootstrapServers must not be null");
        }

        if (events == null || events.length == 0) {
            events = Arrays.stream(EventType.values())
                    .map(Enum::name)
                    .toArray(String[]::new);
        }

        kafkaProperties = KafkaConfig.init(config);
    }

    @Override
    public void postInit(KeycloakSessionFactory arg0) {
        // ignore
    }

    @Override
    public void close() {
        // ignore
        LOG.info("close, shutting down kafka consumer");
        instance.shutDownConsumer();
    }
}