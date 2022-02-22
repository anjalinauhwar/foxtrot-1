package com.flipkart.foxtrot.server.console;

import com.flipkart.foxtrot.common.query.Filter;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TemplateFilter {

    private String table;
    private List<Filter> filters;

}
