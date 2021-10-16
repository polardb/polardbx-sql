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
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.sharding.LabelShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.mapping.Mapping;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * @author chenmo.cm
 */
public class AggregateLabel extends SnapshotLabel {

    protected AggregateLabel(Aggregate rel, Label input, ExtractorContext context) {
        super(LabelType.AGGREGATE, rel, input);

        // immediately apply current aggregate to snapshot label
        aggregate(rel, context);
    }

    protected AggregateLabel(HashGroupJoin rel, Label input, ExtractorContext context) {
        super(LabelType.AGGREGATE, rel, input);

        // immediately apply current aggregate to snapshot label
        aggregate(rel, context);
    }

    protected AggregateLabel(LabelType type, List<Label> inputs, RelNode rel, FullRowType fullRowType,
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

    public static AggregateLabel create(@Nonnull Aggregate agg, Label input, ExtractorContext context) {
        return new AggregateLabel(agg, input, context);
    }

    public static AggregateLabel create(@Nonnull HashGroupJoin agg, Label input, ExtractorContext context) {
        return new AggregateLabel(agg, input, context);
    }

    @Override
    public Label copy(List<Label> inputs) {
        return new AggregateLabel(getType(),
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
