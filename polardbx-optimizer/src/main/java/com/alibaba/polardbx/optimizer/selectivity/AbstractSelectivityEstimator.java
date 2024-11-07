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

package com.alibaba.polardbx.optimizer.selectivity;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;

public abstract class AbstractSelectivityEstimator extends RexVisitorImpl<Double> {

    public final RelMetadataQuery metadataQuery;
    private final RexBuilder rexBuilder;
    protected PlannerContext plannerContext;

    public AbstractSelectivityEstimator(RelMetadataQuery metadataQuery, RexBuilder rexBuilder,
                                        PlannerContext plannerContext) {
        super(true);
        this.metadataQuery = metadataQuery;
        this.rexBuilder = rexBuilder;
        this.plannerContext = plannerContext;
    }

    public static Double normalize(Double selectivity) {
        if (selectivity == null) {
            return null;
        } else if (selectivity < 0) {
            return 0.0;
        } else if (selectivity > 1) {
            return 1.0;
        } else {
            return selectivity;
        }
    }

    public Double evaluate(RexNode predicate) {
        try {
            return evaluateInside(predicate);
        } catch (Throwable e) {
            OptimizerAlertUtil.selectivityAlert(getExecutionContext(), e);
            return null;
        }
    }

    public ExecutionContext getExecutionContext() {
        return plannerContext == null ? null : plannerContext.getExecutionContext();
    }

    public Double evaluateInside(RexNode predicate) {
        if (predicate == null) {
            return 1.0;
        } else {
            RexNode simplifiedPredicate =
                new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, true, RexUtil.EXECUTOR).simplify(predicate);
            if (simplifiedPredicate.isAlwaysTrue()) {
                return 1.0;
            } else if (simplifiedPredicate.isAlwaysFalse()) {
                return 0.0;
            } else {
                Double value = simplifiedPredicate.accept(this);
                if (value == null) {
                    return normalize(RelMdUtil.guessSelectivity(simplifiedPredicate));
                }
                return normalize(value);
            }
        }
    }
}
