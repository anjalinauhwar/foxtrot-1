package com.flipkart.foxtrot.core.events;

import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.phonepe.dataplatform.EventIngestorClient;
import com.phonepe.models.dataplatform.Event;
import io.dropwizard.lifecycle.Managed;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class EventIngestionClient implements Managed {

    private final EventIngestorClient eventIngestorClient;

    @Inject
    public EventIngestionClient(final EventIngestorClient eventIngestorClient) {
        this.eventIngestorClient = eventIngestorClient;
    }

    @Subscribe
    public void publishEvent(Event event) {
        try {
            eventIngestorClient.send(event);
        } catch (Exception e) {
            log.error(String.format("Error ingesting event for reference %s", event.getGroupingKey()), e);
        }
    }

    @Override
    public void start() throws Exception {
        log.info("Starting EventIngestorClient");
        eventIngestorClient.start();
    }

    @Override
    public void stop() throws Exception {
        log.info("Stopping EventIngestorClient");
        eventIngestorClient.close();
    }
}
