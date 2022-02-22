package com.flipkart.foxtrot.core.rebalance;

import java.util.Deque;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShardRebalanceContext {

    private String nodeGroup;

    private String datePostFix;

    private double shardCountThresholdPercentage;

    private Map<String, NodeInfo> nodeNameVsNodeInfo;

    private Map<String, Integer> shardCountPerNode;

    private int totalShards;

    private int avgShardsPerNode;

    private int maxAcceptableShardsPerNode;

    private Deque<String> vacantNodeNames;

}