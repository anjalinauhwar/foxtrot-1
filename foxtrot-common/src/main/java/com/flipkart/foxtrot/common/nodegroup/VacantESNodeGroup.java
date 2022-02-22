package com.flipkart.foxtrot.common.nodegroup;

import static com.flipkart.foxtrot.common.nodegroup.ESNodeGroup.AllocationStatus.VACANT;

import com.flipkart.foxtrot.common.nodegroup.visitors.ESNodeGroupVisitor;

import java.util.SortedSet;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;


@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
public class VacantESNodeGroup extends ESNodeGroup {

    public VacantESNodeGroup() {
        super(VACANT);
    }

    @Builder
    public VacantESNodeGroup(String groupName,
                             SortedSet<String> nodePatterns) {
        super(groupName, VACANT, nodePatterns);
    }

    public <T> T accept(ESNodeGroupVisitor<T> visitor) {
        return visitor.visit(this);
    }

}

