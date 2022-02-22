package com.flipkart.foxtrot.pipeline.resolver;

import io.dropwizard.testing.junit.DropwizardClientRule;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.val;
import org.hibernate.validator.constraints.NotEmpty;
import org.junit.ClassRule;

public class HttpClientPipelineFetcherAdaptorTest extends PipelineFetcherTest {

    @ClassRule
    public static final DropwizardClientRule dropwizard = new DropwizardClientRule(new PipelineResource());

    @Override
    protected PipelineFetcher underTest() {
        val host = dropwizard.baseUri()
                .getHost();
        val port = dropwizard.baseUri()
                .getPort();

        return new HttpClientPipelineFetcherAdaptor(FoxtrotClient.connect(HttpConfiguration.builder()
                .host(host)
                .port(port)
                .basePath(dropwizard.baseUri()
                        .getPath())
                .secure(false)
                .usingZookeeper(false)
                .build(), objectMapper, null));
    }

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
}
