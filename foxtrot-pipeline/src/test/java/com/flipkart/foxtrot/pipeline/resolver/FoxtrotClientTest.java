package com.flipkart.foxtrot.pipeline.resolver;

import static org.junit.Assert.assertNotNull;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.pipeline.PipelineUtils;
import com.flipkart.ranger.ServiceProviderBuilders;
import com.flipkart.ranger.healthcheck.HealthcheckStatus;
import com.google.common.collect.ImmutableSet;
import io.appform.dropwizard.discovery.common.ShardInfo;
import io.dropwizard.testing.junit.DropwizardClientRule;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.val;
import org.apache.curator.test.TestingServer;
import org.hibernate.validator.constraints.NotEmpty;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({FoxtrotClientTest.UrlHttpTest.class, FoxtrotClientTest.RangerHttpTest.class})
public class FoxtrotClientTest {


    @Path("/v1/pipeline")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public static class PipelineResource {

        @GET
        @Path("/{name}")
        public Response ping(@NotEmpty @PathParam("name") String pipeline) {
            if (pipeline.equals(PipelineFetcherTest.PIPELINE_NAME)) {
                return Response.ok(PipelineFetcherTest.pipeline)
                        .build();
            }
            return Response.status(Response.Status.NOT_FOUND)
                    .build();
        }
    }

    public static class RangerHttpTest {

        @ClassRule
        public static final DropwizardClientRule dropwizard = new DropwizardClientRule(
                new FoxtrotClientTest.PipelineResource());
        private TestingServer zkServer;

        @Before
        public void setUp() throws Exception {
            zkServer = new TestingServer(2181, true);
        }

        @After
        public void tearDown() throws Exception {
            zkServer.stop();
        }

        @Test
        public void testClientServiceDiscovery() throws Exception {
            ShardInfo nodeInfo = ShardInfo.builder()
                    .environment("test")
                    .build();
            val mapper = new ObjectMapper();
            PipelineUtils.init(mapper, ImmutableSet.of("com.flipkart.foxtrot"));
            val host = dropwizard.baseUri()
                    .getHost();
            val port = dropwizard.baseUri()
                    .getPort();

            ServiceProviderBuilders.shardedServiceProviderBuilder()
                    .withConnectionString("localhost:2181")
                    .withServiceName("foxtrot")
                    .withNamespace("phonepe")
                    .withNodeData(nodeInfo)
                    .withHostname(host)
                    .withPort(port)
                    .withSerializer((data) -> {
                        try {
                            return mapper.writeValueAsBytes(data);
                        } catch (JsonProcessingException e) {
                            return null;
                        }
                    })
                    .withHealthcheck(() -> HealthcheckStatus.healthy)
                    .buildServiceDiscovery()
                    .start();

            val httpClient = FoxtrotClient.connect(HttpConfiguration.builder()
                    .usingZookeeper(true)
                    .namespace("phonepe")
                    .environment("test")
                    .serviceName("foxtrot")
                    .host("localhost:2181")
                    .basePath(dropwizard.baseUri()
                            .getPath())
                    .build(), mapper, null);
            val pipeline = httpClient.getPipeline(PipelineFetcherTest.PIPELINE_NAME);
            assertNotNull(pipeline);
        }
    }

    public static class UrlHttpTest {

        @ClassRule
        public static final DropwizardClientRule dropwizard = new DropwizardClientRule(
                new FoxtrotClientTest.PipelineResource());


        @Test
        public void testClientServiceDiscovery() throws Exception {
            ShardInfo nodeInfo = ShardInfo.builder()
                    .environment("test")
                    .build();
            val mapper = new ObjectMapper();
            PipelineUtils.init(mapper, ImmutableSet.of("com.flipkart.foxtrot"));
            val host = dropwizard.baseUri()
                    .getHost();
            val port = dropwizard.baseUri()
                    .getPort();

            val httpClient = FoxtrotClient.connect(HttpConfiguration.builder()
                    .usingZookeeper(false)
                    .host(host)
                    .port(port)
                    .basePath(dropwizard.baseUri()
                            .getPath())
                    .build(), mapper, null);
            val pipeline = httpClient.getPipeline(PipelineFetcherTest.PIPELINE_NAME);
            assertNotNull(pipeline);
        }
    }
}