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

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalWindowToSortWindowRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.SortWindow;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

public class COLLogicalWindowToSortWindowRule extends LogicalWindowToSortWindowRule {
    public static final LogicalWindowToSortWindowRule INSTANCE = new COLLogicalWindowToSortWindowRule("INSTANCE");

    public COLLogicalWindowToSortWindowRule(String desc) {
        super("COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    @Override
    protected void createSortWindow(
        RelOptRuleCall call,
        LogicalWindow window,
        RelNode newInput,
        RelCollation relCollation) {
        List<Integer> keyInnerIndexes = Lists.newArrayList();
        for (int i = 0; i < window.groups.size(); i++) {
            Window.Group group = window.groups.get(i);
            if (keyInnerIndexes.size() < group.keys.cardinality()) {
                keyInnerIndexes = group.keys.toList();
            }
        }

        List<Pair<RelTraitSet, RelNode>> implementationList = new ArrayList<>();
        RelDistribution relDistribution;
        switch (keyInnerIndexes.size()) {
        case 0:
            relDistribution = RelDistributions.SINGLETON;
            break;
        case 1:
            relDistribution = RelDistributions.hashOss(keyInnerIndexes,
                PlannerContext.getPlannerContext(window).getColumnarMaxShardCnt());
            // convert by hand
            RelNode sortInput = convert(newInput, window.getCluster().getPlanner().emptyTraitSet()
                .replace(outConvention).replace(relDistribution));
            RelTraitSet traits =
                window.getCluster().getPlanner().emptyTraitSet().replace(relCollation).replace(relDistribution);
            LogicalSort sort =
                LogicalSort.create(traits, sortInput, relCollation, null, null);

            implementationList.add(new Pair<>(
                traits.replace(outConvention),
                convert(sort, traits.replace(outConvention))));
            break;
        default:
            relDistribution = RelDistributions.hash(keyInnerIndexes);
        }

        implementationList.add(new Pair<>(
            window.getCluster().getPlanner().emptyTraitSet()
                .replace(outConvention).replace(relCollation).replace(relDistribution),
            convert(newInput, newInput.getTraitSet()
                .replace(outConvention).replace(relCollation).replace(relDistribution))));

        for (Pair<RelTraitSet, RelNode> implementation : implementationList) {
            SortWindow newWindow =
                SortWindow.create(
                    implementation.left,
                    implementation.right,
                    window.getConstants(),
                    window.groups,
                    window.getRowType());
            call.transformTo(newWindow);
        }
    }
}
