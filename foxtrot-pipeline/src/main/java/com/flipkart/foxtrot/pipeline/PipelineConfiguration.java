package com.flipkart.foxtrot.pipeline;

import java.util.HashSet;
import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PipelineConfiguration {

    @Builder.Default
    private String handlerCacheSpec;
    private Set<String> handlerProcessorPath = new HashSet<>();
}
