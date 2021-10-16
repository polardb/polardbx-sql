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

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.sharding.LabelShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.mapping.Mapping;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * @author chenmo.cm
 */
public class ValuesLabel extends AbstractLabel {

    protected ValuesLabel(@Nonnull RelNode rel, List<Label> inputs) {
        super(LabelType.VALUES, rel, inputs);
    }

    public ValuesLabel(LabelType type, List<Label> inputs, RelNode rel, FullRowType fullRowType, Mapping columnMapping,
                       RelDataType currentBaseRowType, PredicateNode pullUp, PredicateNode pushdown,
                       PredicateNode[] columnConditionMap, List<PredicateNode> predicates) {
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

    public static ValuesLabel create(@Nonnull Values values) {
        return new ValuesLabel(values, ImmutableList.of());
    }

    public static ValuesLabel create(@Nonnull LogicalDynamicValues values) {
        return new ValuesLabel(values, ImmutableList.of());
    }

    @Override
    public Label copy(List<Label> inputs) {
        return new ValuesLabel(getType(),
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

    @Override
    public String toString() {
        final Values values = getRel();
        return super.toString() + ", " + values.toString();
    }
}
