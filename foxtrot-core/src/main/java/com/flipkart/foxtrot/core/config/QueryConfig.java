package com.flipkart.foxtrot.core.config;

import java.util.ArrayList;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;
import lombok.NoArgsConstructor;

/***
 Created by nitish.goyal on 10/09/19
 ***/
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class QueryConfig {

    @Default
    private boolean logQueries = false;

    @Default
    private boolean blockConsoleQueries = false;

    @Default
    private long slowQueryThresholdMs = 1000;

    private List<String> timeoutExceptionMessages = new ArrayList<>();

}
