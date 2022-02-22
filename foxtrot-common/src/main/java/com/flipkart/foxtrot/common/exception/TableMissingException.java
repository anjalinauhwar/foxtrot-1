package com.flipkart.foxtrot.common.exception;

import com.google.common.collect.Maps;

import java.util.Map;

import lombok.Getter;

/**
 * Created by rishabh.goyal on 19/12/15.
 */
@Getter
public class TableMissingException extends FoxtrotException {

    private final String table;

    protected TableMissingException(String table) {
        super(ErrorCode.TABLE_NOT_FOUND);
        this.table = table;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = Maps.newHashMap();
        map.put("table", this.table);
        return map;
    }
}

