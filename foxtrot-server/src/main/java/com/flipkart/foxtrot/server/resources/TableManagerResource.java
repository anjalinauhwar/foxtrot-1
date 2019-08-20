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
package com.flipkart.foxtrot.server.resources;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.Table;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.table.TableManager;
import com.flipkart.foxtrot.gandalf.manager.PermissionManager;
import com.phonepe.platform.http.HttpClientConfiguration;
import com.phonepe.platform.http.OkHttpUtils;
import com.phonepe.platform.http.ServiceEndpointProvider;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import okhttp3.OkHttpClient;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;


@Path("/v1/tables")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Api(value = "/v1/tables")
public class TableManagerResource {

    private final TableManager tableManager;
    private static OkHttpClient okHttp;
    private static ServiceEndpointProvider endpointProvider;
    private static ObjectMapper objectMapper;

    public TableManagerResource(TableManager tableManager) {
        this.tableManager = tableManager;
    }

    public TableManagerResource(TableManager tableManager, final ObjectMapper objectMapper, final MetricRegistry registry, ServiceEndpointProvider serviceEndpointProvider, HttpClientConfiguration httpClientConfiguration) {
        this.objectMapper = objectMapper;
        this.tableManager = tableManager;
        okHttp = OkHttpUtils.createDefaultClient("http-client", registry, httpClientConfiguration);
        endpointProvider = serviceEndpointProvider;
    }

    @POST
    @Timed
    @ApiOperation("Save Table")
    public Response save(@Valid final Table table,
            @QueryParam("forceCreate") @DefaultValue("false") boolean forceCreate) {
        table.setName(ElasticsearchUtils.getValidTableName(table.getName()));
//        tableManager.save(table, forceCreate);
        PermissionManager permissionManager = new PermissionManager(objectMapper, okHttp, endpointProvider);
        permissionManager.manage(table);
        return Response.ok(table)
                .build();
    }

    @GET
    @Timed
    @Path("/{name}")
    @ApiOperation("Get Table")
    public Response get(@PathParam("name") String name) {
        name = ElasticsearchUtils.getValidTableName(name);
        Table table = tableManager.get(name);
        return Response.ok()
                .entity(table)
                .build();
    }

    @PUT
    @Timed
    @Path("/{name}")
    @ApiOperation("Update Table")
    public Response get(@PathParam("name") final String name, @Valid final Table table) {
        table.setName(name);
        tableManager.update(table);
        return Response.ok()
                .build();
    }

    @DELETE
    @Timed
    @Path("/{name}/delete")
    @ApiOperation("Delete Table")
    public Response delete(@PathParam("name") String name) {
        name = ElasticsearchUtils.getValidTableName(name);
        tableManager.delete(name);
        return Response.status(Response.Status.NO_CONTENT)
                .build();
    }

    @GET
    @Timed
    @ApiOperation("Get all Tables")
    public Response getAll() {
        return Response.ok()
                .entity(tableManager.getAll())
                .build();
    }
}
