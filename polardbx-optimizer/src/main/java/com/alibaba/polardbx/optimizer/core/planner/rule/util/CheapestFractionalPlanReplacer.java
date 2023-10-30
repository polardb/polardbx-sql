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

package com.alibaba.polardbx.optimizer.core.planner.rule.util;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import com.alibaba.polardbx.optimizer.core.rel.HashWindow;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.optimizer.core.rel.MaterializedSemiJoin;
import com.alibaba.polardbx.optimizer.core.rel.MemSort;
import com.alibaba.polardbx.optimizer.core.rel.NLJoin;
import com.alibaba.polardbx.optimizer.core.rel.SemiHashJoin;
import com.alibaba.polardbx.optimizer.core.rel.SemiNLJoin;
import com.alibaba.polardbx.optimizer.core.rel.SemiSortMergeJoin;
import com.alibaba.polardbx.optimizer.core.rel.SortAgg;
import com.alibaba.polardbx.optimizer.core.rel.SortMergeJoin;
import com.alibaba.polardbx.optimizer.core.rel.SortWindow;
import com.alibaba.polardbx.optimizer.core.rel.TopN;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author dylan
 */
public class CheapestFractionalPlanReplacer {

    VolcanoPlanner planner;

    public CheapestFractionalPlanReplacer(VolcanoPlanner planner) {
        super();
        this.planner = planner;
    }

    public RelNode visit(RelNode p, double fraction) {
        assert fraction >= 0 && fraction <= 1;
        RelMetadataQuery mq = p.getCluster().getMetadataQuery();
        if (p instanceof RelSubset) {
            RelSubset subset = (RelSubset) p;
            RelNode cheapestTotalPlan = subset.getBest();
            RelNode cheapestStartUpPlan = subset.getBestStartUpRel();

            if (cheapestTotalPlan == null || cheapestStartUpPlan == null) {
                // Dump the planner's expression pool so we can figure
                // out why we reached impasse.
                StringWriter sw = new StringWriter();
                final PrintWriter pw = new PrintWriter(sw);
                pw.println("Node [" + subset.getDescription()
                    + "] could not be implemented; planner state:\n");
                planner.dump(pw);
                pw.flush();
                final String dump = sw.toString();
                RuntimeException e =
                    new RelOptPlanner.CannotPlanException(dump);
                throw e;
            }

            RelOptCost cheapestTotalPlanFractionCost = getFractionalCost(cheapestTotalPlan, fraction, mq);
            RelOptCost cheapestStartUpPlanFractionCost = getFractionalCost(cheapestStartUpPlan, fraction, mq);

            if (cheapestTotalPlanFractionCost.isLt(cheapestStartUpPlanFractionCost.multiplyBy(1.01))) {
                p = cheapestTotalPlan;
            } else {
                p = cheapestStartUpPlan;
            }
        }

        // get Fraction for inputs
        List<Double> fractions = getFraction(p, fraction, mq);

        List<RelNode> oldInputs = p.getInputs();
        List<RelNode> inputs = new ArrayList<>();
        for (int i = 0; i < oldInputs.size(); i++) {
            RelNode oldInput = oldInputs.get(i);
            RelNode input = visit(oldInput, fractions.get(i));
            inputs.add(input);
        }
        if (!inputs.equals(oldInputs)) {
            p = p.copy(p.getTraitSet(), inputs);
        }
        return p;
    }

    private static RelOptCost getFractionalCost(RelNode rel, double fraction, RelMetadataQuery mq) {
        RelOptCost fractionalCost =
            mq.getStartUpCost(rel).plus(
                mq.getCumulativeCost(rel).minus(mq.getStartUpCost(rel)).multiplyBy(fraction));
        return fractionalCost;
    }

    private static List<Double> getFraction(RelNode rel, double fraction, RelMetadataQuery mq) {
        List<Double> result = new ArrayList<>();
        if (rel instanceof Limit) {
            Limit limit = ((Limit) rel);
            Map<Integer, ParameterContext> params =
                PlannerContext.getPlannerContext(rel).getParams().getCurrentParameter();
            long skipPlusFetch = 0;
            if (limit.fetch != null) {
                skipPlusFetch += CBOUtil.getRexParam(limit.fetch, params);

                if (limit.offset != null) {
                    skipPlusFetch += CBOUtil.getRexParam(limit.offset, params);
                }
            }
            fraction = Math.min(1, skipPlusFetch / mq.getRowCount(limit.getInput()));
            result.add(fraction);
        } else if (rel instanceof HashJoin) {
            HashJoin hashJoin = (HashJoin) rel;
            if (hashJoin.getJoinType() == JoinRelType.RIGHT) {
                if (hashJoin.isOuterBuild()) {
                    result.add(fraction);
                    result.add(1.0);
                } else {
                    result.add(1.0);
                    result.add(fraction);
                }
            } else {
                if (hashJoin.isOuterBuild()) {
                    result.add(1.0);
                    result.add(fraction);
                } else {
                    result.add(fraction);
                    result.add(1.0);
                }
            }
        } else if (rel instanceof HashGroupJoin || rel instanceof MaterializedSemiJoin
            || rel instanceof SortMergeJoin || rel instanceof SemiSortMergeJoin) {
            result.add(1.0);
            result.add(1.0);
        } else if (rel instanceof NLJoin) {
            NLJoin nlJoin = (NLJoin) rel;
            if (nlJoin.getJoinType() == JoinRelType.RIGHT) {
                result.add(1.0);
                result.add(fraction);
            } else {
                result.add(fraction);
                result.add(1.0);
            }
        } else if (rel instanceof SemiNLJoin || rel instanceof SemiHashJoin) {
            result.add(fraction);
            result.add(1.0);
        } else if (rel instanceof TopN || rel instanceof MemSort || rel instanceof HashAgg
            || rel instanceof SortWindow || rel instanceof HashWindow || rel instanceof SortAgg) {
            result.add(1.0);
        } else {
            // BKAJoin
            // Project
            // Filter
            // Gather
            // MergeSort
            // non-input rel like LogicalView Values
            // and any other stream relNode
            // just pass through
            for (int i = 0; i < rel.getInputs().size(); i++) {
                result.add(fraction);
            }
        }
        return result;
    }
}
