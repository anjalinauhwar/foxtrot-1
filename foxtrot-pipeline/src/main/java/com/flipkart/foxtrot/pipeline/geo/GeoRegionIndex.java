package com.flipkart.foxtrot.pipeline.geo;

import java.io.IOException;
import java.util.List;

import org.geojson.Feature;

public interface GeoRegionIndex {

    List<Feature> matchingIds(GeoPoint point);

    void index(List<Feature> geoJsonFeatures) throws IOException;
}
