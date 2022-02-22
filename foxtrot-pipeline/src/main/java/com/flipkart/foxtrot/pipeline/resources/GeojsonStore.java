package com.flipkart.foxtrot.pipeline.resources;

import java.util.List;
import java.util.Set;

import org.geojson.Feature;

public interface GeojsonStore {
    List<Feature> fetch(String collectionId, Set<String> ids);
}
