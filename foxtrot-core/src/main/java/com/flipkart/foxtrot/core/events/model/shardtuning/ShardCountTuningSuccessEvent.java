package com.flipkart.foxtrot.core.events.model.shardtuning;

import static com.flipkart.foxtrot.common.Constants.APP_NAME;

import com.flipkart.foxtrot.core.config.ShardCountTuningJobConfig;
import com.flipkart.foxtrot.core.events.model.TrackingEvent;
import com.phonepe.models.dataplatform.Event;

import java.util.Date;
import java.util.UUID;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class ShardCountTuningSuccessEvent extends ShardCountTuningEvent implements
        TrackingEvent<ShardCountTuningSuccessEvent> {


    @Builder
    public ShardCountTuningSuccessEvent(String table,
                                        ShardCountTuningJobConfig shardCountTuningJobConfig,
                                        double maxIndexSizeInGBs,
                                        int idealShardCount,
                                        NodeGroupMetadata nodeGroupMetadata,
                                        String dateVsIndexSize) {
        super("SHARD_COUNT_TUNING_SUCCESS", table, shardCountTuningJobConfig, maxIndexSizeInGBs, idealShardCount,
                nodeGroupMetadata, dateVsIndexSize);
    }

    @Override
    public Event<ShardCountTuningSuccessEvent> toIngestionEvent() {
        return Event.<ShardCountTuningSuccessEvent>builder().groupingKey(this.getTable())
                .eventType(this.getEventType())
                .app(APP_NAME)
                .eventSchemaVersion("v1")
                .id(UUID.randomUUID()
                        .toString())
                .eventData(this)
                .time(new Date())
                .build();
    }

}
