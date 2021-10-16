/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.config.meta;

import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.optimizer.core.rel.TopN;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.GroupJoin;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.List;

// 目前是一个简陋的实现，后续可以不断扩展，但就目前而言是可以满足MPP的简单需求.
// 现在主要用于传递 SINGLETON 属性

public class DrdsRelMdDistribution extends RelMdDistribution {

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
        .reflectiveSource(BuiltInMethod.DISTRIBUTION.method, new DrdsRelMdDistribution());
    //~ Methods ----------------------------------------------------------------

    private DrdsRelMdDistribution() {
        super();
    }

    @Override
    public RelDistribution distribution(RelNode rel, RelMetadataQuery mq) {
        return getDistribution(rel);
    }

    @Override
    public RelDistribution distribution(SingleRel rel, RelMetadataQuery mq) {
        return mq.distribution(rel.getInput());
    }

    @Override
    public RelDistribution distribution(BiRel rel, RelMetadataQuery mq) {
        RelNode left = rel.getLeft();
        RelNode right = rel.getRight();
        RelDistribution leftTrait = mq.distribution(left);
        RelDistribution rightTrait = mq.distribution(right);
        if (leftTrait == RelDistributions.SINGLETON && rightTrait == RelDistributions.SINGLETON) {
            return RelDistributions.SINGLETON;
        } else {
            return getDistribution(rel);
        }
    }

    @Override
    public RelDistribution distribution(SetOp rel, RelMetadataQuery mq) {
        return getDistribution(rel);
    }

    @Override
    public RelDistribution distribution(TableScan scan, RelMetadataQuery mq) {
        RelDistribution distribution = scan.getTable().getDistribution();
        if (distribution != null) {
            return distribution;
        } else {
            return getDistribution(scan);
        }
    }

    @Override
    public RelDistribution distribution(Values values, RelMetadataQuery mq) {
        return RelDistributions.SINGLETON;
    }

    @Override
    public RelDistribution distribution(Project project, RelMetadataQuery mq) {
        RelNode input = project.getInput();
        final RelDistribution inputDistribution = mq.distribution(input);
        List<Integer> keys = inputDistribution.getKeys();
        if (keys.isEmpty()) {
            return inputDistribution;
        }

        final Mappings.TargetMapping mapping = Project
            .getPartialMapping(input.getRowType().getFieldCount(),
                project.getProjects());
        Mapping rawMapping = (Mapping) mapping;
        if (rawMapping.getSourceCount() == keys.size()) {
            final int targetCount = rawMapping.getTargetCount();
            for (int target = 0; target < targetCount; ++target) {
                if (getMappingTargetOpt(rawMapping.inverse(), target) == -1) {
                    return RelDistributions.ANY;
                }
            }
            return inputDistribution.apply(mapping);
        }
        return RelDistributions.ANY;
    }

    //有可能是local sort
    public RelDistribution distribution(Sort sort, RelMetadataQuery mq) {
        return mq.distribution(sort.getInput());
    }

    //有可能是local limit
    public RelDistribution distribution(Limit limit, RelMetadataQuery mq) {
        return mq.distribution(limit.getInput());
    }

    public RelDistribution distribution(TopN topN, RelMetadataQuery mq) {
        return topN.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
    }

    public RelDistribution distribution(Aggregate aggregate, RelMetadataQuery mq) {
        if (aggregate.getGroupCount() != 0) {
            return RelDistributions.ANY;
        } else {
            return RelDistributions.SINGLETON;
        }
    }

    public RelDistribution distribution(GroupJoin aggregate, RelMetadataQuery mq) {
        return RelDistributions.ANY;
    }

    public RelDistribution distribution(LogicalExpand expand, RelMetadataQuery mq) {
        return expand.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
    }

    private RelDistribution getDistribution(RelNode relNode) {
        return relNode.getTraitSet().simplify().getTrait(RelDistributionTraitDef.INSTANCE);
    }

    public int getMappingTargetOpt(Mappings.TargetMapping mapping,
                                   int source) {
        return mapping.getSourceCount() > source ? mapping.getTargetOpt(source) : -1;
    }
}
