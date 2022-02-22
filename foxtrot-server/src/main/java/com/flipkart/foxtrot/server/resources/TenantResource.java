package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.annotation.Timed;
import com.flipkart.foxtrot.common.tenant.Tenant;
import com.flipkart.foxtrot.core.tenant.TenantManager;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/v1/tenant")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/tenant", tags = {"Tenant API"})
@Singleton
public class TenantResource {

    private final TenantManager tenantManager;

    @Inject
    public TenantResource(TenantManager tenantManager) {
        this.tenantManager = tenantManager;
    }

    @GET
    @Timed
    @Path("/{name}")
    @ApiOperation("Get Tenant")
    public Response get(@PathParam("name") String name) {
        Tenant tenant = tenantManager.get(name);
        return Response.ok()
                .entity(tenant)
                .build();
    }

    @GET
    @Timed
    @ApiOperation("Get all Tenant")
    public Response getAll() {
        return Response.ok()
                .entity(tenantManager.getAll())
                .build();
    }

    @POST
    @Timed
    @ApiOperation("Save Tenant")
    public Response save(@Valid final Tenant tenant) {
        tenantManager.save(tenant);
        return Response.ok(tenant)
                .build();
    }

    @POST
    @Path("/bulk")
    @Timed
    @ApiOperation("Save list of tenants")
    public Response saveAll(final List<Tenant> tenants) {
        tenantManager.saveAll(tenants);
        return Response.ok(tenants)
                .build();
    }


    @PUT
    @Timed
    @Path("/{tenantName}")
    @ApiOperation("Update Table")
    public Response get(@PathParam("tenantName") final String tenantName,
                        @Valid final Tenant tenant) {
        tenant.setTenantName(tenantName);
        tenantManager.update(tenant);
        return Response.ok()
                .build();
    }

}
