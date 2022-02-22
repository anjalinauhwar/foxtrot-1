package com.flipkart.foxtrot.core.events.model;

import com.phonepe.models.dataplatform.Event;

public interface TrackingEvent<T> {

    Event<T> toIngestionEvent();
}

