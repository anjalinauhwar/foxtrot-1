package com.flipkart.foxtrot.core.funnel.model.response;

import com.flipkart.foxtrot.core.funnel.model.Funnel;

import java.util.List;

import lombok.Data;

/***
 Created by mudit.g on Jan, 2019
 ***/
@Data
public class FunnelFilterResponse {

    private long hitsCount;

    private List<Funnel> hits;

    public FunnelFilterResponse(long hitsCount,
                                List<Funnel> hits) {
        this.hitsCount = hitsCount;
        this.hits = hits;
    }
}
