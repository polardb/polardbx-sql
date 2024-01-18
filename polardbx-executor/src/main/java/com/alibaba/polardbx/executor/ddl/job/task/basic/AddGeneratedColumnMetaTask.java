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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@TaskName(name = "AddGeneratedColumnMetaTask")
@Getter
public class AddGeneratedColumnMetaTask extends BaseGmsTask {
    private final List<String> sourceExprs;
    private final List<String> targetColumns;

    private final String dbIndex;
    private final String phyTableName;

    private final List<Pair<String, String>> columnAfterAnother;
    private final boolean requireLogicalColumnOrder;

    private final List<String> addedIndexesWithoutNames;

    public AddGeneratedColumnMetaTask(String schemaName, String logicalTableName, List<String> sourceExprs,
                                      List<String> targetColumns, String dbIndex, String phyTableName,
                                      List<Pair<String, String>> columnAfterAnother,
                                      boolean requireLogicalColumnOrder, List<String> addedIndexesWithoutNames) {
        super(schemaName, logicalTableName);
        this.sourceExprs = sourceExprs;
        this.targetColumns = targetColumns;
        this.dbIndex = dbIndex;
        this.phyTableName = phyTableName;
        this.columnAfterAnother = columnAfterAnother;
        this.requireLogicalColumnOrder = requireLogicalColumnOrder;
        this.addedIndexesWithoutNames = addedIndexesWithoutNames;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        // Add column
        TableMetaChanger.addGeneratedColumnMeta(metaDbConnection, schemaName, logicalTableName, dbIndex, phyTableName,
            sourceExprs, targetColumns, columnAfterAnother, requireLogicalColumnOrder, addedIndexesWithoutNames);
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.addGeneratedColumnMetaRollback(metaDbConnection, schemaName, logicalTableName, targetColumns);
    }
}
