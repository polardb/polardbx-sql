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
public class ReplaceRelocateGsiWriter extends ReplaceRelocateWriter implements GsiWriter {
    private final TableMeta gsiMeta;

    public ReplaceRelocateGsiWriter(LogicalInsert parent, RelOptTable targetTable,
                                    DistinctWriter deleteWriter,
                                    DistinctWriter insertWriter,
                                    DistinctWriter modifyWriter, Mapping skTargetMapping,
                                    Mapping skSourceMapping, List<ColumnMeta> skMetas, boolean containsAllUk,
                                    boolean usePartFieldChecker, TableMeta gsiMeta) {
        super(parent, targetTable, deleteWriter, insertWriter, modifyWriter, skTargetMapping, skSourceMapping, skMetas,
            containsAllUk, usePartFieldChecker);
        this.gsiMeta = gsiMeta;
    }

    @Override
    protected void addResult(List<Object> before, Map<Integer, ParameterContext> after, boolean duplicated,
                             boolean pushReplace, boolean doInsert, ExecutionContext ec, ClassifyResult result) {
        if (GlobalIndexMeta.canWrite(ec, gsiMeta)) {
            super.addResult(before, after, duplicated, pushReplace, doInsert, ec, result);
        } else if (GlobalIndexMeta.canDelete(ec, gsiMeta)) {
            // Delete only
            if (duplicated) {
                result.deleteRows.add(before);
            }
        }
    }

    @Override
    public boolean canPushReplace(ExecutionContext ec) {
        return super.canPushReplace(ec) && isGsiPublished(ec);
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
