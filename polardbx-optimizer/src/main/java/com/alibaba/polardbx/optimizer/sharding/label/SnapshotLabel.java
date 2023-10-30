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

import com.alibaba.polardbx.optimizer.sharding.utils.LabelUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.mapping.Mapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * {@link SnapshotLabel} is a snapshot of the child of {@link SnapshotLabel#rel}
 *
 * @author chenmo.cm
 */
public abstract class SnapshotLabel extends AbstractLabel {

    protected SnapshotLabel(LabelType type, RelNode rel, Label input) {
        super(type,
            ImmutableList.of(input),
            rel,
            FullRowType.createSnapshot(input),
            LabelUtil.identityColumnMapping(input.getColumnMapping().getSourceCount()),
            // rel might be a project or an aggregate, so use the input's rowType as currentBaseRowType
            ((AbstractLabel) input).currentBaseRowType,
            null,
            null,
            new PredicateNode[input.getColumnConditionMap().length],
            new ArrayList<>());
    }

    protected SnapshotLabel(LabelType type, List<Label> inputs, RelNode rel, FullRowType fullRowType,
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

    @Override
    public String toString() {
        return LabelUtil.joinNullableString(", ",
            super.toString(),
            Ord.zip(this.columnConditionMap)
                .stream()
                .filter(p -> Objects.nonNull(p.e))
                .map(p -> "[" + LabelUtil.joinNullableString(", ",
                    String.valueOf(p.i),
                    Optional.ofNullable(p.e.getAggCall()).map(AggregateCall::toString).orElse(""),
                    String.join(", ", p.e.getDigests().keySet())) + "]")
                .collect(Collectors.joining(", ")));
    }

    @Override
    public RelDataType deriveRowType() {
        final Label input = getInput(0);
        return deriveRowType(getRel().getCluster().getTypeFactory(), input.getRowType().getFieldList());
    }
}
