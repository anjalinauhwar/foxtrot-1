package com.flipkart.foxtrot.pipeline.processors.geo.utils;

import java.util.Set;

import org.geojson.Feature;

import java.io.IOException;
import java.util.List;

public interface GeoJsonReader {

    List<Feature> fetchFeatures(String sourceURI) throws IOException;

    List<Feature> fetchFeatures(String sourceURI, Set<String> ids) throws IOException;
}
