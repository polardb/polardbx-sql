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

import javax.annotation.Nonnull;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.optimizer.sharding.LabelShuttle;

/**
 * @author chenmo.cm
 * @date 2019-08-09 13:50
 */
public class TableScanLabel extends AbstractLabel {

    protected TableScanLabel(@Nonnull TableScan rel, List<Label> inputs){
        super(LabelType.TABLE_SCAN, rel, inputs);
    }

    public TableScanLabel(LabelType type, List<Label> inputs, RelNode rel, FullRowType fullRowType,
                          Mapping columnMapping, RelDataType currentBaseRowType, PredicateNode pullUp,
                          PredicateNode pushdown, PredicateNode[] columnConditionMap,
                          List<PredicateNode> predicates){
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

    public static TableScanLabel create(@Nonnull TableScan tableScan) {
        return new TableScanLabel(tableScan, ImmutableList.of());
    }

    @Override
    public Label copy(List<Label> inputs) {
        return new TableScanLabel(getType(),
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
        final TableScan tableScan = getRel();
        return super.toString() + ", " + Util.last(tableScan.getTable().getQualifiedName());
    }

    public RelOptTable getTable() {
        return ((TableScan)getRel()).getTable();
    }
}
