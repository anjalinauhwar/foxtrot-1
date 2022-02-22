package com.flipkart.foxtrot.pipeline.resources;

import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class GeojsonFetchRequest {

    private Set<String> ids;
}
