/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.core.querystore.actions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.flipkart.foxtrot.common.Document;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.group.GroupResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.numeric.GreaterThanFilter;
import com.flipkart.foxtrot.common.stats.Stat;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.exception.ErrorCode;
import com.flipkart.foxtrot.core.exception.FoxtrotException;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchQueryStore;
import com.google.common.collect.Maps;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.RequestOptions;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by rishabh.goyal on 28/04/14.
 */
public class GroupActionTest extends ActionTest {

    @BeforeClass
    public static void setUp() throws Exception {
        List<Document> documents = TestUtils.getGroupDocuments(getMapper());
        getQueryStore().save(TestUtils.TEST_TABLE_NAME, documents);
        getElasticsearchConnection().getClient()
                .indices()
                .refresh(new RefreshRequest("*"), RequestOptions.DEFAULT);
        getTableMetadataManager().getFieldMappings(TestUtils.TEST_TABLE_NAME, true, true);
        ((ElasticsearchQueryStore) getQueryStore()).getCardinalityConfig()
                .setMaxCardinality(15000);
        getTableMetadataManager().updateEstimationData(TestUtils.TEST_TABLE_NAME, 1397658117000L);
    }

    @Ignore
    @Test
    public void testGroupActionSingleQueryException() throws FoxtrotException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Collections.singletonList("os"));
        doReturn(null).when(getElasticsearchConnection())
                .getClient();
        try {
            getQueryExecutor().execute(groupRequest);
            fail();
        } catch (FoxtrotException ex) {
            ex.printStackTrace();
            assertEquals(ErrorCode.ACTION_EXECUTION_ERROR, ex.getCode());
        }
    }

    @Test
    public void testGroupActionSingleFieldNoFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Collections.singletonList("os"));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", 7L);
        response.put("ios", 4L);

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionSingleFieldEmptyFieldNoFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Collections.singletonList(""));

        try {
            getQueryExecutor().execute(groupRequest);
            fail();
        } catch (FoxtrotException ex) {
            ex.printStackTrace();
            assertEquals(ErrorCode.MALFORMED_QUERY, ex.getCode());
        }
    }

    @Test
    public void testGroupActionSingleFieldSpecialCharactersNoFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Collections.singletonList(""));

        try {
            getQueryExecutor().execute(groupRequest);
            fail();
        } catch (FoxtrotException ex) {
            ex.printStackTrace();
            assertEquals(ErrorCode.MALFORMED_QUERY, ex.getCode());
        }
    }

    @Test
    public void testGroupActionSingleFieldHavingSpecialCharactersWithFilter()
            throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("device");
        equalsFilter.setValue("nexus");
        groupRequest.setFilters(Collections.<Filter>singletonList(equalsFilter));
        groupRequest.setNesting(Collections.singletonList("!@#$%^&*()"));

        Map<String, Object> response = Maps.newHashMap();

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionSingleFieldWithFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);

        EqualsFilter equalsFilter = new EqualsFilter();
        equalsFilter.setField("device");
        equalsFilter.setValue("nexus");
        groupRequest.setFilters(Collections.<Filter>singletonList(equalsFilter));
        groupRequest.setNesting(Collections.singletonList("os"));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", 5L);
        response.put("ios", 1L);

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionTwoFieldsNoFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device"));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", new HashMap<String, Object>() {{
            put("nexus", 5L);
            put("galaxy", 2L);
        }});
        response.put("ios", new HashMap<String, Object>() {{
            put("nexus", 1L);
            put("ipad", 2L);
            put("iphone", 1L);
        }});

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionTwoFieldsWithFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device"));

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        groupRequest.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", new HashMap<String, Object>() {{
            put("nexus", 3L);
            put("galaxy", 2L);
        }});
        response.put("ios", new HashMap<String, Object>() {{
            put("ipad", 1L);
        }});

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionMultipleFieldsNoFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        Map<String, Object> response = Maps.newHashMap();

        final Map<String, Object> nexusResponse = new HashMap<String, Object>() {{
            put("1", 2L);
            put("2", 2L);
            put("3", 1L);
        }};
        final Map<String, Object> galaxyResponse = new HashMap<String, Object>() {{
            put("2", 1L);
            put("3", 1L);
        }};
        response.put("android", new HashMap<String, Object>() {{
            put("nexus", nexusResponse);
            put("galaxy", galaxyResponse);
        }});

        final Map<String, Object> nexusResponse2 = new HashMap<String, Object>() {{
            put("2", 1L);
        }};
        final Map<String, Object> iPadResponse = new HashMap<String, Object>() {{
            put("2", 2L);
        }};
        final Map<String, Object> iPhoneResponse = new HashMap<String, Object>() {{
            put("1", 1L);
        }};
        response.put("ios", new HashMap<String, Object>() {{
            put("nexus", nexusResponse2);
            put("ipad", iPadResponse);
            put("iphone", iPhoneResponse);
        }});

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(response, actualResult.getResult());
    }

    @Test
    public void testGroupActionMultipleFieldsWithFilter() throws FoxtrotException, JsonProcessingException {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "device", "version"));

        GreaterThanFilter greaterThanFilter = new GreaterThanFilter();
        greaterThanFilter.setField("battery");
        greaterThanFilter.setValue(48);
        groupRequest.setFilters(Collections.<Filter>singletonList(greaterThanFilter));

        Map<String, Object> response = Maps.newHashMap();

        final Map<String, Object> nexusResponse = new HashMap<String, Object>() {{
            put("2", 2L);
            put("3", 1L);
        }};
        final Map<String, Object> galaxyResponse = new HashMap<String, Object>() {{
            put("2", 1L);
            put("3", 1L);
        }};
        response.put("android", new HashMap<String, Object>() {{
            put("nexus", nexusResponse);
            put("galaxy", galaxyResponse);
        }});

        final Map<String, Object> iPadResponse = new HashMap<String, Object>() {{
            put("2", 1L);
        }};
        response.put("ios", new HashMap<String, Object>() {{
            put("ipad", iPadResponse);
        }});

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(response, actualResult.getResult());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGroupActionDistinctCountAggregation() {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "version"));
        groupRequest.setUniqueCountOn("device");

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", new HashMap<String, Object>() {{
            put("1", 1L);
            put("2", 2L);
            put("3", 2L);
        }});
        response.put("ios", new HashMap<String, Object>() {{
            put("1", 1L);
            put("2", 2L);
        }});
        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(((Map<String, Object>) response.get("android")).get("1"),
                ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("1"));
        assertEquals(((Map<String, Object>) response.get("android")).get("2"),
                ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("2"));
        assertEquals(((Map<String, Object>) response.get("android")).get("3"),
                ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("3"));
        assertEquals(((Map<String, Object>) response.get("ios")).get("1"),
                ((Map<String, Object>) actualResult.getResult()
                        .get("ios")).get("1"));
        assertEquals(((Map<String, Object>) response.get("ios")).get("2"),
                ((Map<String, Object>) actualResult.getResult()
                        .get("ios")).get("2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGroupActionMaxAggregation() {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "version"));
        groupRequest.setAggregationField("battery");
        groupRequest.setAggregationType(Stat.MAX);

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", new HashMap<String, Object>() {{
            put("1", 48.0);
            put("2", 99.0);
            put("3",87.0);
        }});
        response.put("ios", new HashMap<String, Object>() {{
            put("1", 24.0);
            put("2",56.0);
        }});

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(((Map<String, Object>) response.get("android")).get("1"), ((Map<String, Object>) actualResult.getResult()
                .get("android")).get("1"));
        assertEquals(((Map<String, Object>) response.get("android")).get("2"), ((Map<String, Object>) actualResult.getResult()
                .get("android")).get("2"));
        assertEquals(((Map<String, Object>) response.get("android")).get("3"), ((Map<String, Object>) actualResult.getResult()
                .get("android")).get("3"));
        assertEquals(((Map<String, Object>) response.get("ios")).get("1"), ((Map<String, Object>) actualResult.getResult()
                .get("ios")).get("1"));
        assertEquals(((Map<String, Object>) response.get("ios")).get("2"), ((Map<String, Object>) actualResult.getResult()
                .get("ios")).get("2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGroupActionAvgAggregation() {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "version"));
        groupRequest.setAggregationField("battery");
        groupRequest.setAggregationType(Stat.AVG);

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", new HashMap<String, Object>() {{
            put("1", 36.0);
            put("2", 84.33333333333333);
            put("3", 80.5);
        }});
        response.put("ios", new HashMap<String, Object>() {{
            put("1", 24.0);
            put("2", 45.0);
        }});

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(((Map<String, Object>) response.get("android")).get("1"),
                ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("1"));
        assertEquals(((Map<String, Object>) response.get("android")).get("2"),
                ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("2"));
        assertEquals(((Map<String, Object>) response.get("android")).get("3"),
                ((Map<String, Object>) actualResult.getResult()
                        .get("android")).get("3"));
        assertEquals(((Map<String, Object>) response.get("ios")).get("1"),
                ((Map<String, Object>) actualResult.getResult()
                        .get("ios")).get("1"));
        assertEquals(((Map<String, Object>) response.get("ios")).get("2"), ((Map<String, Object>) actualResult.getResult()
                .get("ios")).get("2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGroupActionSumAggregation() {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "version"));
        groupRequest.setAggregationField("battery");
        groupRequest.setAggregationType(Stat.SUM);

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", new HashMap<String, Object>() {{
            put("1",72.0);
            put("2", 253.0);
            put("3",161.0);
        }});
        response.put("ios", new HashMap<String, Object>() {{
            put("1", 24.0);
            put("2", 135.0);
        }});

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(((Map<String, Object>) response.get("android")).get("1"), ((Map<String, Object>) actualResult.getResult()
                .get("android")).get("1"));
        assertEquals(((Map<String, Object>) response.get("android")).get("2"), ((Map<String, Object>) actualResult.getResult()
                .get("android")).get("2"));
        assertEquals(((Map<String, Object>) response.get("android")).get("3"), ((Map<String, Object>) actualResult.getResult()
                .get("android")).get("3"));
        assertEquals(((Map<String, Object>) response.get("ios")).get("1"), ((Map<String, Object>) actualResult.getResult()
                .get("ios")).get("1"));
        assertEquals(((Map<String, Object>) response.get("ios")).get("2"), ((Map<String, Object>) actualResult.getResult()
                .get("ios")).get("2"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testGroupActionCountAggregation() {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setTable(TestUtils.TEST_TABLE_NAME);
        groupRequest.setNesting(Arrays.asList("os", "version"));
        groupRequest.setAggregationField("battery");
        groupRequest.setAggregationType(Stat.COUNT);

        Map<String, Object> response = Maps.newHashMap();
        response.put("android", new HashMap<String, Object>() {{
            put("1", 2L);
            put("2",3L);
            put("3", 2L);
        }});
        response.put("ios", new HashMap<String, Object>() {{
            put("1", 1L);
            put("2", 3L);
        }});

        GroupResponse actualResult = (GroupResponse) getQueryExecutor().execute(groupRequest);
        assertEquals(((Map<String, Object>) response.get("android")).get("1"), ((Map<String, Object>) actualResult.getResult()
                .get("android")).get("1"));
        assertEquals(((Map<String, Object>) response.get("android")).get("2"), ((Map<String, Object>) actualResult.getResult()
                .get("android")).get("2"));
        assertEquals(((Map<String, Object>) response.get("android")).get("3"), ((Map<String, Object>) actualResult.getResult()
                .get("android")).get("3"));
        assertEquals(((Map<String, Object>) response.get("ios")).get("1"), ((Map<String, Object>) actualResult.getResult()
                .get("ios")).get("1"));
        assertEquals(((Map<String, Object>) response.get("ios")).get("2"), ((Map<String, Object>) actualResult.getResult()
                .get("ios")).get("2"));
    }

}
