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
public class ShardCountStatusCheckEvent implements TrackingEvent<ShardCountStatusCheckEvent> {

    private String nodeGroup;

    private String datePostFix;

    private String shardCountPerNode;

    private int totalShards;

    private int avgShardsPerNode;

    private int maxAcceptableShardsPerNode;

    private double shardCountThresholdPercentage;

    @Override
    public Event<ShardCountStatusCheckEvent> toIngestionEvent() {
        return Event.<ShardCountStatusCheckEvent>builder().groupingKey(nodeGroup)
                .eventType("SHARD_COUNT_STATUS_CHECK")
                .app(APP_NAME)
                .eventSchemaVersion("v1")
                .id(UUID.randomUUID()
                        .toString())
                .eventData(this)
                .time(new Date())
                .build();
    }
}