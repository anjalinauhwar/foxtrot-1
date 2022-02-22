package com.flipkart.foxtrot.pipeline.resources;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.flipkart.foxtrot.pipeline.processors.geo.utils.GeoJsonReader;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import javax.ws.rs.WebApplicationException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class GeojsonStoreImplTest {

    private GeojsonStoreImpl underTest;
    private GeoJsonReader mockReader = mock(GeoJsonReader.class);

    @Before
    public void setUp() throws Exception {
        underTest = new GeojsonStoreImpl(mockReader, new GeojsonStoreConfiguration(ImmutableList.of(
                GeojsonCollectionSource.builder()
                        .collectionId("collectionId")
                        .name("name")
                        .uri("uri")
                        .build())));
    }

    @Test(expected = WebApplicationException.class)
    public void testFetchNonExistentCollection() {
        underTest.fetch("nonExistentCollectionId", null);
    }

    @Test
    public void testFetchCollection() throws IOException {
        underTest.fetch("collectionId", null);
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(mockReader, atLeastOnce()).fetchFeatures(captor.capture(), any());
        Assert.assertEquals("uri", captor.getValue());
    }
}