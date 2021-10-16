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
import java.util.Map;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.util.mapping.Mapping;

/**
 * @author chenmo.cm
 * @date 2020/2/11 10:36 下午
 */
public class FilterSubqueryWrapperLabel extends SubqueryWrapperLabel {

    protected FilterSubqueryWrapperLabel(LogicalFilter filter, Label input,
                                         Map<RexSubQuery, SubqueryLabel> subqueryLabelMap,
                                         Set<CorrelationId> variablesSet){
        super(LabelType.FILTER_SUBQUERY, filter, input, subqueryLabelMap, variablesSet);
    }

    protected FilterSubqueryWrapperLabel(LabelType type, List<Label> inputs, RelNode rel, FullRowType fullRowType,
                                         Mapping columnMapping, RelDataType currentBaseRowType, PredicateNode pullUp,
                                         PredicateNode pushdown, PredicateNode[] columnConditionMap,
                                         List<PredicateNode> predicates,
                                         Map<RexSubQuery, SubqueryLabel> subqueryLabelMap,
                                         Set<CorrelationId> variablesSet){
        super(type,
            inputs,
            rel,
            fullRowType,
            columnMapping,
            currentBaseRowType,
            pullUp,
            pushdown,
            columnConditionMap,
            predicates,
            subqueryLabelMap,
            variablesSet);
    }

    public static FilterSubqueryWrapperLabel create(LogicalFilter filter, Label input,
                                                    Map<RexSubQuery, SubqueryLabel> subqueryLabelMap,
                                                    Set<CorrelationId> variablesSet) {
        return new FilterSubqueryWrapperLabel(filter, input, subqueryLabelMap, variablesSet);
    }

    public LogicalFilter getFilter() {
        return getRel();
    }

    @Override
    public Label copy(List<Label> inputs) {
        return new FilterSubqueryWrapperLabel(getType(),
            inputs,
            rel,
            fullRowType,
            columnMapping,
            currentBaseRowType,
            pullUp,
            pushdown,
            columnConditionMap,
            predicates,
            subqueryLabelMap,
            variablesSet);
    }
}
