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

import com.alibaba.polardbx.optimizer.sharding.LabelShuttle;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.mapping.Mapping;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

/**
 * @author chenmo.cm
 * @date 2020/2/12 9:58 上午
 */
public abstract class SubqueryWrapperLabel extends SnapshotLabel implements Label {

    /**
     * Map of subquery which is the operand of AND expression
     */
    protected final Map<RexSubQuery, SubqueryLabel> subqueryLabelMap;
    protected final Set<CorrelationId>              variablesSet;

    protected SubqueryWrapperLabel(LabelType type, RelNode rel, Label input,
                                   Map<RexSubQuery, SubqueryLabel> subqueryLabelMap, Set<CorrelationId> variablesSet){
        super(type, rel, input);
        this.subqueryLabelMap = new LinkedHashMap<>(Optional.ofNullable(subqueryLabelMap).orElseGet(ImmutableMap::of));
        this.variablesSet = new LinkedHashSet<>(Optional.ofNullable(variablesSet).orElseGet(ImmutableSet::of));
    }

    protected SubqueryWrapperLabel(LabelType type, List<Label> inputs, RelNode rel, FullRowType fullRowType,
                                   Mapping columnMapping, RelDataType currentBaseRowType, PredicateNode pullUp,
                                   PredicateNode pushdown, PredicateNode[] columnConditionMap,
                                   List<PredicateNode> predicates, Map<RexSubQuery, SubqueryLabel> subqueryLabelMap,
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
            predicates);
        this.subqueryLabelMap = subqueryLabelMap;
        this.variablesSet = variablesSet;
    }


    @Override
    public Label accept(LabelShuttle shuttle) {
        return shuttle.visit(this);
    }

    public void addSubQuery(RexSubQuery subQuery, SubqueryLabel label) {
        subqueryLabelMap.put(subQuery, label);
    }

    public SubqueryWrapperLabel addInferredCorrelationId(Set<CorrelationId> correlationIds) {
        variablesSet.addAll(correlationIds);
        return this;
    }

    public boolean isCorrelate() {
        return variablesSet != null && !variablesSet.isEmpty();
    }

    public Map<RexSubQuery, SubqueryLabel> getSubqueryLabelMap() {
        return subqueryLabelMap;
    }

    public Map<RexSubQuery, SubqueryLabel> getInferableSubqueryLabelMap() {
        final Map<RexSubQuery, SubqueryLabel> result = new HashMap<>();
        subqueryLabelMap.forEach((k, v) -> {
            if (k.getKind() != SqlKind.NOT_IN
                && k.getKind() != SqlKind.NOT_EXISTS) {
                result.put(k, v);
            }
        });
        return result;
    }

    public Set<CorrelationId> getVariablesSet() {
        return variablesSet;
    }

    public Label copy(List<Label> inputs,
                      Map<RexSubQuery, SubqueryLabel> subqueryLabelMap,
                      Set<CorrelationId> variablesSet) {
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

    public SubqueryWrapperLabel subqueryAccept(LabelShuttle shuttle) {
        final LinkedHashMap<RexSubQuery, SubqueryLabel> newSubqueryLabelMap = new LinkedHashMap<>();

        boolean updated = false;
        for (Entry<RexSubQuery, SubqueryLabel> entry : subqueryLabelMap.entrySet()) {
            final SubqueryLabel visited = (SubqueryLabel)shuttle.visit(entry.getValue());

            newSubqueryLabelMap.put(entry.getKey(), visited);

            if (entry.getValue() != visited) {
                updated = true;
            }
        }

        if (updated) {
            return (SubqueryWrapperLabel)copy(getInputs(), newSubqueryLabelMap, getVariablesSet());
        }

        return this;
    }
}
