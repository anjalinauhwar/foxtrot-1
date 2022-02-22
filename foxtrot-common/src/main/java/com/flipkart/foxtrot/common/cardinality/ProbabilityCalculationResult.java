package com.flipkart.foxtrot.common.cardinality;

import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ProbabilityCalculationResult {

    private double probability;
    private long estimatedMaxDocCount;
    private long estimatedDocCountBasedOnTime;
    private long estimatedDocCountAfterFilters;
    private Map<String, Long> groupingColumnCardinality;
    private long outputCardinality;
    private long maxCardinality;
    private boolean cardinalityReduced;

}