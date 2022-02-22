package com.flipkart.foxtrot.pipeline.resolver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.pipeline.Pipeline;
import com.google.common.base.Strings;
import feign.Client;
import feign.Feign;
import feign.Logger;
import feign.Param;
import feign.Request;
import feign.RequestLine;
import feign.Retryer;
import feign.Target;
import feign.codec.Decoder;
import feign.codec.Encoder;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.okhttp.OkHttpClient;
import feign.ranger.RangerTarget;

import java.util.concurrent.TimeUnit;

import lombok.SneakyThrows;
import lombok.val;
import okhttp3.ConnectionPool;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

public interface FoxtrotClient {

    static FoxtrotClient connect(final HttpConfiguration httpConfiguration,
                                 final ObjectMapper mapper,
                                 CuratorFramework curatorFramework) {
        final Decoder decoder = new JacksonDecoder(mapper);
        final Encoder encoder = new JacksonEncoder(mapper);

        Target<FoxtrotClient> target = null;
        if (httpConfiguration.isUsingZookeeper() && httpConfiguration.isValidZkDiscoveryConfig()) {
            curatorFramework = startCuratorFramework(curatorFramework, httpConfiguration);
            target = buildRangerTarget(mapper, curatorFramework, httpConfiguration);
        } else {
            target = new Target.HardCodedTarget<>(FoxtrotClient.class, httpConfiguration.getUrl());
        }
        if (!Strings.isNullOrEmpty(httpConfiguration.getBasePath())) {
            target = new BasePathTarget<>(target, httpConfiguration.getBasePath());
        }
        return Feign.builder()
                .encoder(encoder)
                .decoder(decoder)
                .requestInterceptor(requestTemplate -> requestTemplate.header("Content-Type", "application/json"))
                .retryer(new Retryer.Default(10, 50, 1))
                .options(new Request.Options(httpConfiguration.getConnectTimeoutMs(),
                        httpConfiguration.getOpTimeoutMs()))
                .logger(new Logger.JavaLogger())
                .logLevel(Logger.Level.BASIC)
                .client(createOkHttpClient(httpConfiguration))
                .target(target);

    }

    static CuratorFramework startCuratorFramework(CuratorFramework curatorFramework,
                                                  HttpConfiguration httpConfiguration) {
        if (curatorFramework == null) {
            curatorFramework = CuratorFrameworkFactory.builder()
                    .connectString(httpConfiguration.getHost())
                    .namespace(httpConfiguration.getNamespace())
                    .retryPolicy(new RetryForever(httpConfiguration.getRetryInterval()))
                    .build();
            curatorFramework.start();
            return curatorFramework;
        }
        return curatorFramework;
    }

    @SneakyThrows
    static RangerTarget<FoxtrotClient> buildRangerTarget(ObjectMapper mapper,
                                                         CuratorFramework curatorFramework,
                                                         HttpConfiguration httpConfiguration) {
        return new RangerTarget<>(FoxtrotClient.class, httpConfiguration.getEnvironment(),
                httpConfiguration.getNamespace(), httpConfiguration.getServiceName(), curatorFramework,
                httpConfiguration.isSecure(), mapper);
    }

    static Client createOkHttpClient(final HttpConfiguration httpConfig) {
        okhttp3.OkHttpClient.Builder clientBuilder = new okhttp3.OkHttpClient.Builder();
        clientBuilder.retryOnConnectionFailure(true);
        final int connections = httpConfig.getConnections();
        final int idleTimeOutSeconds = httpConfig.getIdleTimeOutSeconds();
        final int connTimeout = httpConfig.getConnectTimeoutMs();
        final int opTimeout = httpConfig.getOpTimeoutMs();
        okhttp3.OkHttpClient client = clientBuilder.connectionPool(
                new ConnectionPool(connections, idleTimeOutSeconds, TimeUnit.SECONDS))
                .connectTimeout(connTimeout, TimeUnit.MILLISECONDS)
                .readTimeout(opTimeout, TimeUnit.MILLISECONDS)
                .writeTimeout(opTimeout, TimeUnit.MILLISECONDS)
                .build();
        return new OkHttpClient(client);
    }

    @RequestLine("GET /v1/pipeline/{pipeline}")
    Pipeline getPipeline(@Param("pipeline") String pipelineName);
}
