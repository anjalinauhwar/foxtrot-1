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
public class ShardRebalanceFinishEvent implements TrackingEvent<ShardRebalanceFinishEvent> {

    private String nodeGroup;

    private String nodeName;

    private int initialShardCount;

    private int finalShardCount;

    private String datePostFix;

    @Override
    public Event<ShardRebalanceFinishEvent> toIngestionEvent() {
        return Event.<ShardRebalanceFinishEvent>builder().groupingKey(nodeGroup)
                .eventType("SHARD_REBALANCE_FINISHED")
                .app(APP_NAME)
                .eventSchemaVersion("v1")
                .id(UUID.randomUUID()
                        .toString())
                .eventData(this)
                .time(new Date())
                .build();
    }

}
