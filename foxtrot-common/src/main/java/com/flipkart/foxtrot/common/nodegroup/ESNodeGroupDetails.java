package com.flipkart.foxtrot.common.nodegroup;

import com.fasterxml.jackson.annotation.JsonUnwrapped;

import java.util.SortedSet;
import java.util.TreeMap;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ESNodeGroupDetails {

    private int nodeCount;

    private TreeMap<String, DiskUsageInfo> nodeInfo;

    @JsonUnwrapped
    private DiskUsageInfo diskUsageInfo;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class DiskUsageInfo {

        private String totalDiskStorage;

        private String usedDiskStorage;

        private String availableDiskStorage;

        private String usedDiskPercentage;
    }
}
