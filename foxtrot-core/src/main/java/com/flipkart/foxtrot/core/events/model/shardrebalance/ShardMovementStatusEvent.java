package com.flipkart.foxtrot.core.events.model.shardrebalance;

import static com.flipkart.foxtrot.common.Constants.APP_NAME;

import com.flipkart.foxtrot.core.events.model.TrackingEvent;
import com.phonepe.models.dataplatform.Event;

import java.util.Date;
import java.util.UUID;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ShardMovementStatusEvent implements TrackingEvent<ShardMovementStatusEvent> {

    private String nodeGroup;

    private String datePostFix;

    private String moveCommand;

    private String fromNode;

    private String toNode;

    private boolean acknowledged;

    @Override
    public Event<ShardMovementStatusEvent> toIngestionEvent() {
        return Event.<ShardMovementStatusEvent>builder().groupingKey(nodeGroup)
                .eventType("SHARD_MOVEMENT_STATUS")
                .app(APP_NAME)
                .eventSchemaVersion("v1")
                .id(UUID.randomUUID()
                        .toString())
                .eventData(this)
                .time(new Date())
                .build();
    }
}
