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

import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorContext;
import com.alibaba.polardbx.optimizer.sharding.utils.LabelUtil;
import com.alibaba.polardbx.optimizer.sharding.utils.PredicateUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public abstract class AbstractLabel extends AbstractLabelOptNode {

    protected AbstractLabel(LabelType type, @Nonnull RelNode rel,
                            List<Label> inputs) {
        super(type, rel, inputs);
    }

    protected AbstractLabel(LabelType type,
                            List<Label> inputs, RelNode rel,
                            FullRowType fullRowType, Mapping columnMapping,
                            RelDataType currentBaseRowType,
                            PredicateNode pullUp,
                            PredicateNode pushdown,
                            PredicateNode[] columnConditionMap,
                            List<PredicateNode> predicates) {
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
    }

    @Override
    public Label project(Project project, ExtractorContext context) {
        final List<Pair<Integer, RexNode>> newColumnRexMap = new ArrayList<>();
        final Mapping partialMapping = LabelUtil.getInversePartialMapping(project, newColumnRexMap);

        final boolean isPermutation = newColumnRexMap.isEmpty();

        if (!isPermutation) {
            // Need ProjectLabel for inference
            Preconditions.checkState(withoutExpressionColumn());
        }

        // Get snapshot for PredicateNode before columnConditionMap updated
        final Label baseLabelSnapshot = isPermutation ? null : clone();

        // Permute current columnConditionMap
        this.columnConditionMap = LabelUtil.inverseApply(partialMapping, this.columnConditionMap, PredicateNode.class);

        // Add new expression column to columnCondition Map
        newColumnRexMap.forEach(pair -> this.columnConditionMap[pair.getKey()] = new PredicateNode(baseLabelSnapshot,
            null,
            ImmutableList.of(pair.getValue()),
            context));

        // Permute current columnMapping
        this.columnMapping = LabelUtil.multiply(partialMapping, this.columnMapping);
        this.currentBaseRowType = project.getRowType();

        // Clear rowType
        this.rowType = null;

        return this;
    }

    @Override
    public Label window(Window window, ExtractorContext context) {
        final Mapping partialMapping = LabelUtil.getInversePartialMapping(window);

        // Get snapshot for PredicateNode before columnConditionMap updated
        final Label baseLabelSnapshot = null;

        // Permute current columnConditionMap
        this.columnConditionMap = LabelUtil.inverseApply(partialMapping, this.columnConditionMap, PredicateNode.class);

        // Permute current columnMapping
        this.columnMapping = LabelUtil.multiply(partialMapping, this.columnMapping);
        this.currentBaseRowType = window.getRowType();

        // Clear rowType
        this.rowType = null;

        return this;
    }

    @Override
    public Label expand(LogicalExpand logicalExpand, ExtractorContext context) {
        final Mapping partialMapping = LabelUtil.getInversePartialMapping(logicalExpand);

        // Permute current columnConditionMap
        this.columnConditionMap = LabelUtil.inverseApply(partialMapping, this.columnConditionMap, PredicateNode.class);

        // Permute current columnMapping
        this.columnMapping = LabelUtil.multiply(partialMapping, this.columnMapping);
        this.currentBaseRowType = logicalExpand.getRowType();

        // Clear rowType
        this.rowType = null;

        return this;
    }

    @Override
    public Label aggregate(Aggregate agg, ExtractorContext context) {
        final List<Pair<Integer, AggregateCall>> aggCalls = new ArrayList<>();
        final Mapping partialMapping = LabelUtil.getInversePartialMapping(agg, aggCalls);

        final boolean withoutAggCall = aggCalls.isEmpty();

        if (!withoutAggCall) {
            // Need AggregationLabel for inference
            Preconditions.checkState(withoutExpressionColumn());
        }

        // Get snapshot for PredicateNode before columnConditionMap updated
        final Label baseLabelSnapshot = withoutAggCall ? null : clone();

        // Permute current columnConditionMap
        this.columnConditionMap = LabelUtil.inverseApply(partialMapping, this.columnConditionMap, PredicateNode.class);

        // Add aggregate call to columnCondition Map
        aggCalls.forEach(pair -> this.columnConditionMap[pair.getKey()] = new PredicateNode(baseLabelSnapshot,
            agg,
            pair.getValue(),
            context));

        // Permute current columnMapping
        this.columnMapping = LabelUtil.multiply(partialMapping, this.columnMapping);
        this.currentBaseRowType = agg.getRowType();

        // Clear rowType
        this.rowType = null;

        return this;
    }

    @Override
    public Label aggregate(HashGroupJoin agg, ExtractorContext context) {
        final List<Pair<Integer, AggregateCall>> aggCalls = new ArrayList<>();
        final Mapping partialMapping = LabelUtil.getInversePartialMapping(agg, aggCalls);

        final boolean withoutAggCall = aggCalls.isEmpty();

        if (!withoutAggCall) {
            // Need AggregationLabel for inference
            Preconditions.checkState(withoutExpressionColumn());
        }

        // Get snapshot for PredicateNode before columnConditionMap updated
        final Label baseLabelSnapshot = withoutAggCall ? null : clone();

        // Permute current columnConditionMap
        this.columnConditionMap = LabelUtil.inverseApply(partialMapping, this.columnConditionMap, PredicateNode.class);

        // Add aggregate call to columnCondition Map
        aggCalls.forEach(pair -> this.columnConditionMap[pair.getKey()] = new PredicateNode(baseLabelSnapshot,
            agg,
            pair.getValue(),
            context));

        // Permute current columnMapping
        this.columnMapping = LabelUtil.multiply(partialMapping, this.columnMapping);
        this.currentBaseRowType = agg.getRowType();

        // Clear rowType
        this.rowType = null;

        return this;
    }

    @Override
    public Label filter(RexNode predicate, Filter filter,
                        ExtractorContext context) {
        final Map<String, RexNode> deduplicated = PredicateUtil.deduplicate(predicate, context);

        // Clone current label
        this.predicates.add(new PredicateNode(this.clone(), filter, deduplicated, context));

        return this;
    }

}
