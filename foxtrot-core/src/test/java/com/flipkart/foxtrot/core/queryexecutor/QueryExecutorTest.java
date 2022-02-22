/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.flipkart.foxtrot.core.queryexecutor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.Query;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.exception.ErrorCode;
import com.flipkart.foxtrot.common.exception.FoxtrotException;
import com.flipkart.foxtrot.common.group.GroupRequest;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.EqualsFilter;
import com.flipkart.foxtrot.common.query.string.ContainsFilter;
import com.flipkart.foxtrot.common.util.JsonUtils;
import com.flipkart.foxtrot.common.util.SerDe;
import com.flipkart.foxtrot.core.TestUtils;
import com.flipkart.foxtrot.core.cache.CacheManager;
import com.flipkart.foxtrot.core.cache.impl.DistributedCacheFactory;
import com.flipkart.foxtrot.core.cardinality.CardinalityValidator;
import com.flipkart.foxtrot.core.cardinality.CardinalityValidatorImpl;
import com.flipkart.foxtrot.core.common.RequestWithNoAction;
import com.flipkart.foxtrot.core.common.noncacheable.NonCacheableAction;
import com.flipkart.foxtrot.core.common.noncacheable.NonCacheableActionRequest;
import com.flipkart.foxtrot.core.config.QueryConfig;
import com.flipkart.foxtrot.core.datastore.DataStore;
import com.flipkart.foxtrot.core.email.EmailConfig;
import com.flipkart.foxtrot.core.events.EventBusManager;
import com.flipkart.foxtrot.core.events.EventIngestionClient;
import com.flipkart.foxtrot.core.events.model.query.FullTextSearchQueryEvent;
import com.flipkart.foxtrot.core.events.model.query.QueryTimeoutEvent;
import com.flipkart.foxtrot.core.events.model.query.SlowQueryEvent;
import com.flipkart.foxtrot.core.funnel.config.BaseFunnelEventConfig;
import com.flipkart.foxtrot.core.funnel.config.FunnelConfiguration;
import com.flipkart.foxtrot.core.funnel.persistence.ElasticsearchFunnelStore;
import com.flipkart.foxtrot.core.funnel.services.MappingService;
import com.flipkart.foxtrot.core.internalevents.impl.GuavaInternalEventBus;
import com.flipkart.foxtrot.core.querystore.ActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.EventPublisherActionExecutionObserver;
import com.flipkart.foxtrot.core.querystore.QueryStore;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.ElasticsearchTuningConfig;
import com.flipkart.foxtrot.core.querystore.handlers.MetricRecorder;
import com.flipkart.foxtrot.core.querystore.handlers.ResponseCacheUpdater;
import com.flipkart.foxtrot.core.querystore.handlers.SlowQueryReporter;
import com.flipkart.foxtrot.core.querystore.impl.CacheConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.core.table.TableMetadataManager;
import com.flipkart.foxtrot.core.util.FunnelExtrapolationUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.EventBus;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.phonepe.dataplatform.EventIngestorClient;
import com.phonepe.models.dataplatform.Event;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.flipkart.foxtrot.common.enums.SourceType.SERVICE;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * Created by rishabh.goyal on 02/05/14.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest(RestHighLevelClient.class)
public class QueryExecutorTest {

    private static HazelcastInstance hazelcastInstance;
    @Rule
    public ExpectedException exception = ExpectedException.none();
    private QueryExecutor queryExecutor;
    private ObjectMapper mapper = new ObjectMapper();
    private AnalyticsLoader analyticsLoader;
    private QueryExecutorFactory queryExecutorFactory;
    private ExecutorService executorService;
    private FunnelConfiguration funnelConfiguration;
    private ElasticsearchFunnelStore funnelStore;
    private CacheManager cacheManager;
    private EventIngestorClient eventIngestorClient;
    private RestHighLevelClient restHighLevelClient;

    @BeforeClass
    public static void setupClass() {
        hazelcastInstance = new TestHazelcastInstanceFactory(1).newHazelcastInstance(new Config());

    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        hazelcastInstance.shutdown();
    }

    @Before
    public void setUp() throws Exception {
        ElasticsearchConnection elasticsearchConnection = PowerMockito.mock(ElasticsearchConnection.class);
        restHighLevelClient = PowerMockito.mock(RestHighLevelClient.class);
        ElasticsearchConfig elasticsearchConfig = new ElasticsearchConfig();
        PowerMockito.when(elasticsearchConnection.getClient())
                .thenReturn(restHighLevelClient);
        PowerMockito.when(elasticsearchConnection.getConfig())
                .thenReturn(elasticsearchConfig);

        DataStore dataStore = TestUtils.getDataStore();

        //Initializing Cache Factory
        HazelcastConnection hazelcastConnection = mock(HazelcastConnection.class);
        when(hazelcastConnection.getHazelcast()).thenReturn(hazelcastInstance);
        when(hazelcastConnection.getHazelcastConfig()).thenReturn(new Config());
        this.cacheManager = new CacheManager(
                new DistributedCacheFactory(hazelcastConnection, mapper, new CacheConfig()));
        TableMetadataManager tableMetadataManager = mock(TableMetadataManager.class);
        when(tableMetadataManager.exists(ArgumentMatchers.anyString())).thenReturn(true);
        when(tableMetadataManager.get(ArgumentMatchers.anyString())).thenReturn(TestUtils.TEST_TABLE);
        QueryStore queryStore = mock(QueryStore.class);
        CardinalityValidator cardinalityValidator = new CardinalityValidatorImpl(queryStore, tableMetadataManager);

        analyticsLoader = spy(
                new AnalyticsLoader(tableMetadataManager, dataStore, queryStore, elasticsearchConnection, cacheManager,
                        mapper, new ElasticsearchTuningConfig(), cardinalityValidator));
        TestUtils.registerActions(analyticsLoader, mapper);

        SerDe.init(mapper);
        this.executorService = Executors.newFixedThreadPool(1);

        this.funnelConfiguration = FunnelConfiguration.builder()
                .baseFunnelEventConfig(BaseFunnelEventConfig.builder()
                        .eventType("APP_LOADED")
                        .category("APP_LOADED")
                        .build())
                .build();

        this.eventIngestorClient = Mockito.mock(EventIngestorClient.class);

        EventBus eventBus = new AsyncEventBus(Executors.newCachedThreadPool());
        EventBusManager eventBusManager = new EventBusManager(eventBus, new EventIngestionClient(eventIngestorClient));
        eventBusManager.start();

        QueryConfig queryConfig = QueryConfig.builder()
                .slowQueryThresholdMs(0)
                .timeoutExceptionMessages(Collections.singletonList("listener timeout after waiting"))
                .build();

        EmailConfig emailConfig = EmailConfig.builder()
                .build();

        List<ActionExecutionObserver> actionExecutionObservers = ImmutableList.<ActionExecutionObserver>builder().add(
                new MetricRecorder())
                .add(new ResponseCacheUpdater(cacheManager))
                .add(new SlowQueryReporter(queryConfig))
                .add(new EventPublisherActionExecutionObserver(new GuavaInternalEventBus(), eventBusManager,
                        queryConfig))
                .build();

        queryExecutor = new SimpleQueryExecutor(analyticsLoader, executorService, actionExecutionObservers);
        MappingService mappingService = new MappingService(elasticsearchConnection, funnelConfiguration);
        this.funnelStore = new ElasticsearchFunnelStore(elasticsearchConnection, mappingService, funnelConfiguration);
        queryExecutorFactory = new QueryExecutorFactory(analyticsLoader, executorService,
                Collections.singletonList(new ResponseCacheUpdater(cacheManager)), funnelConfiguration, funnelStore);

    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testResolve() throws Exception {
        assertEquals(NonCacheableAction.class, queryExecutor.resolve(new NonCacheableActionRequest())
                .getClass());
    }

    @Test
    public void testResolveNonExistentAction() throws Exception {
        try {
            queryExecutor.resolve(new RequestWithNoAction());
            fail();
        } catch (FoxtrotException e) {
            assertEquals(ErrorCode.UNRESOLVABLE_OPERATION, e.getCode());
        }
    }

    @Test
    public void testSimpleExecutorFactory() {
        ActionRequest actionRequest = new Query();
        QueryExecutor queryExecutor = queryExecutorFactory.getExecutor(actionRequest);
        if (queryExecutor instanceof SimpleQueryExecutor) {
            Assert.assertTrue(true);
        } else {
            Assert.fail();
        }
    }

    @Test
    public void testSimpleExecutorFactoryWithFlagEnabled() {
        ActionRequest actionRequest = new Query();
        funnelConfiguration.setEnabled(true);
        queryExecutorFactory = new QueryExecutorFactory(analyticsLoader, executorService,
                Collections.singletonList(new ResponseCacheUpdater(cacheManager)), funnelConfiguration, funnelStore);
        QueryExecutor queryExecutor = queryExecutorFactory.getExecutor(actionRequest);
        if (queryExecutor instanceof SimpleQueryExecutor) {
            Assert.assertTrue(true);
        } else {
            Assert.fail();
        }
    }

    @Test
    public void testExtrapolationQueryExecutorFactoryWithQuery() {
        ActionRequest actionRequest = new Query();
        Filter filter = EqualsFilter.builder()
                .field(FunnelExtrapolationUtils.FUNNEL_ID_QUERY_FIELD)
                .value("123")
                .build();
        actionRequest.getFilters()
                .add(filter);
        funnelConfiguration.setEnabled(true);
        queryExecutorFactory = new QueryExecutorFactory(analyticsLoader, executorService,
                Collections.singletonList(new ResponseCacheUpdater(cacheManager)), funnelConfiguration, funnelStore);
        QueryExecutor queryExecutor = queryExecutorFactory.getExecutor(actionRequest);
        if (queryExecutor instanceof SimpleQueryExecutor) {
            Assert.assertTrue(true);
        } else {
            Assert.fail();
        }
    }

    @Test
    public void testExtrapolationQueryExecutorFactoryWithCountRequest() {
        ActionRequest actionRequest = new CountRequest();
        Filter filter = EqualsFilter.builder()
                .field(FunnelExtrapolationUtils.FUNNEL_ID_QUERY_FIELD)
                .value("123")
                .build();
        actionRequest.getFilters()
                .add(filter);
        actionRequest.setExtrapolationFlag(true);
        funnelConfiguration.setEnabled(true);
        queryExecutorFactory = new QueryExecutorFactory(analyticsLoader, executorService,
                Collections.singletonList(new ResponseCacheUpdater(cacheManager)), funnelConfiguration, funnelStore);
        QueryExecutor queryExecutor = queryExecutorFactory.getExecutor(actionRequest);
        if (queryExecutor instanceof ExtrapolationQueryExecutor) {
            Assert.assertTrue(true);
        } else {
            Assert.fail();
        }
    }

    @Test
    public void shouldIngestQueryTimeoutEvent() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setSourceType(SERVICE);
        groupRequest.setRequestTags(ImmutableMap.of("serviceName", "anomaly-detection"));
        groupRequest.setTable("payment");
        groupRequest.setFilters(Collections.singletonList(EqualsFilter.builder()
                .value("PAYMENT_COMPLETED")
                .field("state")
                .build()));

        groupRequest.setNesting(Collections.singletonList("eventType"));

        Mockito.when(restHighLevelClient.search(Mockito.any(SearchRequest.class)))
                .thenThrow(new IOException("listener timeout after waiting for 90000 ms"));

        try {
            queryExecutor.execute(groupRequest);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof FoxtrotException);
        }
        final ArgumentCaptor<Event> argumentCaptor = ArgumentCaptor.forClass(Event.class);
        await().pollDelay(2000, TimeUnit.MILLISECONDS)
                .until(() -> true);
        Mockito.verify(eventIngestorClient, Mockito.times(1))
                .send(argumentCaptor.capture());

        Event publishedEvent = argumentCaptor.getValue();

        Object eventData = publishedEvent.getEventData();
        Assert.assertTrue(eventData instanceof QueryTimeoutEvent);
        QueryTimeoutEvent queryTimeoutEvent = (QueryTimeoutEvent) eventData;

        Assert.assertEquals(JsonUtils.toJson(groupRequest), queryTimeoutEvent.getActionRequest());
        Assert.assertEquals("group", queryTimeoutEvent.getOpCode());
        Assert.assertEquals(SERVICE, queryTimeoutEvent.getSourceType());
        Assert.assertEquals("payment", queryTimeoutEvent.getTable());

        Assert.assertTrue(queryTimeoutEvent.getRequestTags()
                .containsKey("serviceName"));
        Assert.assertEquals("anomaly-detection", queryTimeoutEvent.getRequestTags()
                .get("serviceName"));

    }

    @Test
    public void shouldIngestSlowQueryEvent() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setSourceType(SERVICE);
        groupRequest.setRequestTags(ImmutableMap.of("serviceName", "anomaly-detection"));
        groupRequest.setTable("payment");
        groupRequest.setFilters(Collections.singletonList(EqualsFilter.builder()
                .value("PAYMENT_COMPLETED")
                .field("state")
                .build()));

        groupRequest.setNesting(Collections.singletonList("eventType"));

        SearchResponse searchResponse = Mockito.mock(SearchResponse.class);
        Mockito.when(searchResponse.getAggregations())
                .thenReturn(null);
        Mockito.when(restHighLevelClient.search(Mockito.any(SearchRequest.class)))
                .thenReturn(searchResponse);

        queryExecutor.execute(groupRequest);
        await().pollDelay(2000, TimeUnit.MILLISECONDS)
                .until(() -> true);
        final ArgumentCaptor<Event> argumentCaptor = ArgumentCaptor.forClass(Event.class);
        Mockito.verify(eventIngestorClient, Mockito.atLeast(1))
                .send(argumentCaptor.capture());

        List<Event> events = argumentCaptor.getAllValues();

        Assert.assertTrue(events.stream()
                .anyMatch(event -> event.getEventData() instanceof SlowQueryEvent));

        Optional<Event> optionalEvent = events.stream()
                .filter(event -> event.getEventData() instanceof SlowQueryEvent)
                .findFirst();

        Event publishedEvent = optionalEvent.get();
        Object eventData = publishedEvent.getEventData();
        Assert.assertTrue(eventData instanceof SlowQueryEvent);
        SlowQueryEvent slowQueryEvent = (SlowQueryEvent) eventData;

        Assert.assertEquals(JsonUtils.toJson(groupRequest), slowQueryEvent.getActionRequest());
        Assert.assertEquals("group", slowQueryEvent.getOpCode());
        Assert.assertEquals(SERVICE, slowQueryEvent.getSourceType());
        Assert.assertEquals("payment", slowQueryEvent.getTable());

        Assert.assertTrue(slowQueryEvent.getRequestTags()
                .containsKey("serviceName"));
        Assert.assertEquals("anomaly-detection", slowQueryEvent.getRequestTags()
                .get("serviceName"));
    }

    @Test
    public void shouldIngestFullTextSearchQueryEvent() throws Exception {
        GroupRequest groupRequest = new GroupRequest();
        groupRequest.setSourceType(SERVICE);
        groupRequest.setRequestTags(ImmutableMap.of("serviceName", "anomaly-detection"));
        groupRequest.setTable("payment");
        ContainsFilter containsFilter = new ContainsFilter();
        containsFilter.setField("eventData.channel");
        containsFilter.setValue("*android*");

        groupRequest.setFilters(Collections.singletonList(containsFilter));

        groupRequest.setNesting(Collections.singletonList("eventType"));

        SearchResponse searchResponse = Mockito.mock(SearchResponse.class);
        Mockito.when(searchResponse.getAggregations())
                .thenReturn(null);
        Mockito.when(restHighLevelClient.search(Mockito.any(SearchRequest.class)))
                .thenReturn(searchResponse);

        queryExecutor.execute(groupRequest);
        await().pollDelay(2000, TimeUnit.MILLISECONDS)
                .until(() -> true);
        final ArgumentCaptor<Event> argumentCaptor = ArgumentCaptor.forClass(Event.class);
        Mockito.verify(eventIngestorClient, Mockito.atLeast(1))
                .send(argumentCaptor.capture());

        List<Event> events = argumentCaptor.getAllValues();

        Assert.assertTrue(events.stream()
                .anyMatch(event -> event.getEventData() instanceof FullTextSearchQueryEvent));

        Optional<Event> optionalEvent = events.stream()
                .filter(event -> event.getEventData() instanceof FullTextSearchQueryEvent)
                .findFirst();
        Event publishedEvent = optionalEvent.get();
        Object eventData = publishedEvent.getEventData();
        Assert.assertTrue(eventData instanceof FullTextSearchQueryEvent);
        FullTextSearchQueryEvent fullTextSearchQueryEvent = (FullTextSearchQueryEvent) eventData;

        Assert.assertEquals(JsonUtils.toJson(groupRequest), fullTextSearchQueryEvent.getActionRequest());
        Assert.assertEquals("group", fullTextSearchQueryEvent.getOpCode());
        Assert.assertEquals(SERVICE, fullTextSearchQueryEvent.getSourceType());
        Assert.assertEquals("payment", fullTextSearchQueryEvent.getTable());
        Assert.assertTrue(fullTextSearchQueryEvent.getFullTextSearchFields()
                .contains("eventData.channel"));
        Assert.assertTrue(fullTextSearchQueryEvent.getRequestTags()
                .containsKey("serviceName"));
        Assert.assertEquals("anomaly-detection", fullTextSearchQueryEvent.getRequestTags()
                .get("serviceName"));
    }
}
