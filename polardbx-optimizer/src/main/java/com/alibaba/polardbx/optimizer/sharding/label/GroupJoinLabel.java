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
import com.google.common.base.Preconditions;
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.sharding.LabelShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.Mapping;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xiaoying
 */
public class GroupJoinLabel extends SnapshotLabel {

    protected GroupJoinLabel(HashGroupJoin rel, Label input, ExtractorContext context) {
        super(LabelType.AGGREGATE, rel, input);

        // immediately apply current aggregate to snapshot label
        aggregate(rel, context);
    }

    protected GroupJoinLabel(LabelType type, List<Label> inputs, RelNode rel, FullRowType fullRowType,
                             Mapping columnMapping, RelDataType currentBaseRowType, PredicateNode pullUp,
                             PredicateNode pushdown, PredicateNode[] columnConditionMap,
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

    public static GroupJoinLabel create(@Nonnull HashGroupJoin agg, Label input, ExtractorContext context) {
        return new GroupJoinLabel(agg, input, context);
    }

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
    public Label copy(List<Label> inputs) {
        return new GroupJoinLabel(getType(),
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
    public Label accept(LabelShuttle shuttle) {
        return shuttle.visit(this);
    }
}
