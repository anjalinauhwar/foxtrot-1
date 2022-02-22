package com.flipkart.foxtrot.common.nodegroup.visitors;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class MoveTablesRequest {

    private String sourceGroup;

    private String destinationGroup;

    private List<String> tables;
}
