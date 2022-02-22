package com.flipkart.foxtrot.server.providers.exception;

import com.flipkart.foxtrot.server.auth.JWTAuthenticationFailure;

import java.net.URI;
import javax.inject.Singleton;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import lombok.extern.slf4j.Slf4j;

/**
 *
 */
@Provider
@Singleton
@Slf4j
public class AuthenticationExceptionHandler implements ExceptionMapper<JWTAuthenticationFailure> {

    @Override
    public Response toResponse(JWTAuthenticationFailure exception) {
        log.error("Authentication failure: " + exception.getMessage(), exception);
        return Response.seeOther(URI.create("/foxtrot/oauth/login")).build();
    }
}
