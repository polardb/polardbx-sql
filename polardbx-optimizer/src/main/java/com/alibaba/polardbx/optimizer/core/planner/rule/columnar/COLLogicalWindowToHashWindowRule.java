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

package com.alibaba.polardbx.optimizer.core.planner.rule.columnar;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.LogicalWindowToHashWindowRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.HashWindow;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.List;

public class COLLogicalWindowToHashWindowRule extends LogicalWindowToHashWindowRule {
    public static final LogicalWindowToHashWindowRule INSTANCE = new COLLogicalWindowToHashWindowRule("INSTANCE");

    public COLLogicalWindowToHashWindowRule(String desc) {
        super("COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    public void createHashWindow(
        RelOptRuleCall call,
        LogicalWindow window,
        RelNode newInput) {
        List<Pair<RelDistribution, RelDistribution>> implementationList = new ArrayList<>();
        ImmutableBitSet groupIndex = window.groups.get(0).keys;
        if (groupIndex.cardinality() == 0) {
            implementationList.add(Pair.of(RelDistributions.SINGLETON, RelDistributions.SINGLETON));
        } else {
            if (PlannerContext.getPlannerContext(window).getParamManager()
                .getBoolean(ConnectionParams.ENABLE_PARTITION_WISE_WINDOW)) {
                int inputLoc = -1;
                for (int i = 0; i < groupIndex.cardinality(); i++) {
                    inputLoc = groupIndex.nextSetBit(inputLoc + 1);
                    RelDistribution windowDistribution = RelDistributions.hashOss(ImmutableList.of(inputLoc),
                        PlannerContext.getPlannerContext(window).getColumnarMaxShardCnt());
                    RelDistribution inputDistribution = RelDistributions.hashOss(ImmutableList.of(inputLoc),
                        PlannerContext.getPlannerContext(window).getColumnarMaxShardCnt());
                    implementationList.add(Pair.of(windowDistribution, inputDistribution));
                }
            } else {
                RelDistribution windowDistribution =
                    RelDistributions.hash(groupIndex.toList());
                RelDistribution inputDistribution = RelDistributions.hash(groupIndex.toList());
                implementationList.add(Pair.of(windowDistribution, inputDistribution));
            }
        }
        for (Pair<RelDistribution, RelDistribution> implementation : implementationList) {
            HashWindow newWindow = HashWindow.create(
                window.getTraitSet().replace(outConvention).replace(implementation.getKey()),
                convert(newInput, newInput.getTraitSet().replace(implementation.getValue())),
                window.getConstants(),
                window.groups,
                window.getRowType());
            call.transformTo(newWindow);
        }
    }
}
