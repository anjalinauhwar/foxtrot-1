package com.flipkart.foxtrot.common.nodegroup;

import com.flipkart.foxtrot.common.elasticsearch.node.NodeFSStatsResponse.NodeFSDetails;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class VacantGroupReadRepairResult {

    private List<NodeFSDetails> dataNodes;
    private VacantESNodeGroup vacantNodeGroup;
}
