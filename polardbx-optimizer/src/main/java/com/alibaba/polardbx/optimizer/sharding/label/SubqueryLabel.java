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

package com.alibaba.polardbx.optimizer.sharding.label;

import java.util.List;

import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorContext;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.util.mapping.Mapping;

import com.alibaba.polardbx.optimizer.sharding.LabelShuttle;

/**
 * @author chenmo.cm
 * @date 2020/2/12 10:07 上午
 */
public class SubqueryLabel extends SnapshotLabel {

    protected final RexSubQuery subquery;

    /**
     * for IN/NOT IN subquery
     */
    protected PredicateNode     inferredCorrelateCondition;

    protected SubqueryLabel(Label input, RexSubQuery subquery){
        super(LabelType.REX_SUBQUERY, subquery.rel, input);
        this.subquery = subquery;
    }

    protected SubqueryLabel(LabelType type, List<Label> inputs, RelNode rel, FullRowType fullRowType,
                            Mapping columnMapping, RelDataType currentBaseRowType, PredicateNode pullUp,
                            PredicateNode pushdown, PredicateNode[] columnConditionMap,
                            List<PredicateNode> predicates, RexSubQuery subquery,
                            PredicateNode inferredCorrelateCondition){
        super(type,
            inputs,
            rel,
            fullRowType,
            columnMapping,
            currentBaseRowType,
            pullUp,
            pushdown,
            columnConditionMap,
            predicates);
        this.subquery = subquery;
        this.inferredCorrelateCondition = inferredCorrelateCondition;
    }
    
    public static SubqueryLabel create(Label input, RexSubQuery subquery,
                                       List<RexNode> inferredCorrelateCondition, ExtractorContext context) {
        final SubqueryLabel result = new SubqueryLabel(input, subquery);
        final SubqueryLabel clone = (SubqueryLabel) result.clone();

        final PredicateNode inferredCorrelation = new PredicateNode(clone, null, inferredCorrelateCondition, context);

        result.setInferredCorrelateCondition(inferredCorrelation);
        clone.setInferredCorrelateCondition(inferredCorrelation);

        return result;
    }

    @Override
    public Label copy(List<Label> inputs) {
        return new SubqueryLabel(getType(),
            inputs,
            rel,
            fullRowType,
            columnMapping,
            currentBaseRowType,
            pullUp,
            pushdown,
            columnConditionMap,
            predicates,
            subquery,
            inferredCorrelateCondition);
    }

    @Override
    public Label accept(LabelShuttle shuttle) {
        return shuttle.visit(this);
    }

    public PredicateNode getInferredCorrelateCondition() {
        return inferredCorrelateCondition;
    }

    private void setInferredCorrelateCondition(PredicateNode inferredCorrelateCondition) {
        this.inferredCorrelateCondition = inferredCorrelateCondition;
    }

    public RexSubQuery getSubquery() {
        return subquery;
    }
}
