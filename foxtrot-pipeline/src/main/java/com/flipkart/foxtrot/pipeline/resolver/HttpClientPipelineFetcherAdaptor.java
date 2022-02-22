package com.flipkart.foxtrot.pipeline.resolver;

import com.flipkart.foxtrot.pipeline.Pipeline;
import feign.FeignException;

import javax.ws.rs.core.Response;

public class HttpClientPipelineFetcherAdaptor implements PipelineFetcher {

    private final FoxtrotClient foxtrotClient;

    public HttpClientPipelineFetcherAdaptor(FoxtrotClient foxtrotClient) {
        this.foxtrotClient = foxtrotClient;
    }

    @Override
    public Pipeline fetch(String id) {
        try {
            return foxtrotClient.getPipeline(id);
        } catch (FeignException feignException) {
            if (feignException.status() == Response.Status.NOT_FOUND.getStatusCode()) {
                return null;
            }
            throw feignException;
        }
    }
}
