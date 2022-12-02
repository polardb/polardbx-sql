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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.GsiWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.ClassifyResult;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.util.mapping.Mapping;

import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public class UpsertRelocateGsiWriter extends UpsertRelocateWriter implements GsiWriter {
    private final TableMeta gsiMeta;

    public UpsertRelocateGsiWriter(LogicalInsert parent,
                                   RelOptTable targetTable,
                                   InsertWriter simpleInsertWriter,
                                   InsertWriter distinctInsertWriter,
                                   DistinctWriter relocateDeleteWriter,
                                   DistinctWriter relocateInsertWriter,
                                   DistinctWriter modifyWriter,
                                   Mapping skTargetMapping,
                                   Mapping skSourceMapping,
                                   List<ColumnMeta> skMetas,
                                   boolean modifySkOnly,
                                   boolean usePartFieldChecker,
                                   TableMeta gsiMeta) {
        super(parent, targetTable, simpleInsertWriter, distinctInsertWriter, relocateDeleteWriter, relocateInsertWriter,
            modifyWriter, skTargetMapping, skSourceMapping, skMetas, modifySkOnly, usePartFieldChecker);
        this.gsiMeta = gsiMeta;
    }

    @Override
    protected void addResult(List<Object> before, List<Object> after, List<Object> merged,
                             Map<Integer, ParameterContext> insertParam, boolean duplicated, boolean insertThenUpdate,
                             boolean doUpdate, ExecutionContext ec, ClassifyResult result) {
        if (GlobalIndexMeta.canWrite(ec, getGsiMeta())) {
            super.addResult(before, after, merged, insertParam, duplicated, insertThenUpdate, doUpdate, ec, result);
        } else if (GlobalIndexMeta.canDelete(ec, getGsiMeta())) {
            // DELETE ONLY
            if (duplicated && !insertThenUpdate) {
                // INSERT then UPDATE means two insert rows are duplicated, just skip them both
                result.relocateBeforeRows.add(before);
            }
        }
    }

    @Override
    public TableMeta getGsiMeta() {
        return gsiMeta;
    }

    @Override
    public boolean isGsiPublished(ExecutionContext ec) {
        return GlobalIndexMeta.isPublished(ec, getGsiMeta());
    }
}
