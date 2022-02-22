package com.flipkart.foxtrot.pipeline.resolver;

import feign.Request;
import feign.RequestTemplate;
import feign.Target;

public class BasePathTarget<T> implements Target<T> {

    private final Target<T> delegate;
    private final String basePath;

    public BasePathTarget(Target<T> delegate,
                          String basePath) {
        this.delegate = delegate;
        this.basePath = basePath;
    }

    @Override
    public Class<T> type() {
        return delegate.type();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public String url() {
        return delegate.url() + basePath;
    }

    @Override
    public Request apply(RequestTemplate input) {
        input.target(url());
        return input.request();
    }
}
