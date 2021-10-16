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

package com.alibaba.polardbx.optimizer.core.rel;

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

public class LogicalDynamicValues extends DynamicValues {

    protected LogicalDynamicValues(RelOptCluster cluster, RelTraitSet traits, RelDataType rowType,
                                   ImmutableList<ImmutableList<RexNode>> tuples) {
        super(cluster, traits, rowType, tuples);
    }

    public LogicalDynamicValues(RelInput input) {
        super(input.getCluster(), input.getTraitSet(), input.getRowType("type"), input.getDynamicTuples("tuples"));
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LogicalDynamicValues(getCluster(), traitSet, rowType, tuples);
    }

    public static LogicalDynamicValues createDrdsValues(
        RelOptCluster cluster, RelTraitSet traits, RelDataType rowType, ImmutableList<ImmutableList<RexNode>> tuples) {
        return new LogicalDynamicValues(cluster, traits.replace(DrdsConvention.INSTANCE), rowType, tuples);
    }
}
