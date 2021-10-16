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

package com.alibaba.polardbx.optimizer.core.rel.dml.writer;

import com.google.common.base.Preconditions;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableModifyBuilder;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.mapping.Mapping;

import java.util.List;
import java.util.function.Function;

/**
 * Insert writer, support deduplicate input rows
 *
 * @author chenmo.cm
 */
public class DistinctInsertWriter extends InsertWriter implements DistinctWriter {

    private final Mapping deduplicateMapping;

    public DistinctInsertWriter(RelOptTable targetTable, LogicalInsert insert, Mapping deduplicateMapping) {
        super(targetTable, insert);
        Preconditions.checkNotNull(insert);

        this.deduplicateMapping = deduplicateMapping;
    }

    @Override
    public Mapping getGroupingMapping() {
        return deduplicateMapping;
    }

    @Override
    public List<RelNode> getInput(ExecutionContext ec, Function<DistinctWriter, List<List<Object>>> rowGenerator) {
        // Deduplicate
        final List<List<Object>> distinctRows = rowGenerator.apply(this);

        return PhyTableModifyBuilder.buildInsert(insert, distinctRows, ec, false);
    }
}
