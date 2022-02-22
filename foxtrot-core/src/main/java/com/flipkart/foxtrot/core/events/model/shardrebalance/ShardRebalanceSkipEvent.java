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
public class ShardRebalanceSkipEvent implements TrackingEvent<ShardRebalanceSkipEvent> {

    private String nodeGroup;

    private String nodeName;

    private int shardCount;

    private String datePostFix;

    private int avgShardsPerNode;

    private int maxAcceptableShardsPerNode;

    private ShardRebalanceSkipReason skipReason;

    @Override
    public Event<ShardRebalanceSkipEvent> toIngestionEvent() {
        return Event.<ShardRebalanceSkipEvent>builder().groupingKey(nodeGroup)
                .eventType("SHARD_REBALANCE_SKIP")
                .app(APP_NAME)
                .eventSchemaVersion("v1")
                .id(UUID.randomUUID()
                        .toString())
                .eventData(this)
                .time(new Date())
                .build();
    }

    public enum ShardRebalanceSkipReason {
        NO_VACANT_NODES,
        ALREADY_BALANCED
    }
}
