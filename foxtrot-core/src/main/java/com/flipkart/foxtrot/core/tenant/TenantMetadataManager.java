package com.flipkart.foxtrot.core.tenant;

import io.dropwizard.lifecycle.Managed;
import com.flipkart.foxtrot.common.tenant.Tenant;

import java.util.List;

public interface TenantMetadataManager extends Managed {

    void save(Tenant tenant);

    Tenant get(String tenantName);

    List<String> getEmailIds(String tenantName);

    List<Tenant> get();

    boolean exists(String tableName);
}
