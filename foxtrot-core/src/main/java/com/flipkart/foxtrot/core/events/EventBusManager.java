package com.flipkart.foxtrot.core.events;

import com.google.common.eventbus.EventBus;
import com.phonepe.models.dataplatform.Event;
import io.dropwizard.lifecycle.Managed;

import javax.inject.Inject;
import javax.inject.Singleton;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class EventBusManager implements Managed {

    @Getter
    private final EventBus eventBus;

    private final EventIngestionClient eventIngestionClient;

    @Inject
    public EventBusManager(final EventBus eventBus,
                           final EventIngestionClient eventIngestionClient) {
        this.eventBus = eventBus;
        this.eventIngestionClient = eventIngestionClient;
    }

    @Override
    public void start() {
        log.info("Starting EventBusManager");
        eventBus.register(eventIngestionClient);
    }

    @Override
    public void stop() {
        log.info("Stopping EventBusManager");
        eventBus.unregister(eventIngestionClient);
    }

    public void postEvent(Event event) {
        eventBus.post(event);
    }
}
