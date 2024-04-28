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

package com.alibaba.polardbx.executor.ddl.job.task.onlinemodifycolumn;

import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.executor.ddl.job.meta.GsiMetaChanger.updateIndexColumnMeta;

@Getter
@TaskName(name = "OnlineModifyColumnSwapMetaTask")
public class OnlineModifyColumnSwapMetaTask extends BaseGmsTask {
    private final boolean changeColumn;

    private final String newColumnName;
    private final String oldColumnName;
    private final String afterColumnName;

    private final String dbIndex;
    private final String phyTableName;

    private final Map<String, List<String>> localIndexes;
    private final Map<String, String> uniqueIndexMap;

    private final List<String> coveringGsi;
    private final List<String> gsiDbIndex;
    private final List<String> gsiPhyTableName;

    private final boolean nullable;

    public OnlineModifyColumnSwapMetaTask(String schemaName, String logicalTableName, boolean changeColumn,
                                          String dbIndex, String phyTableName, String newColumnName,
                                          String oldColumnName, String afterColumnName,
                                          Map<String, List<String>> localIndexes, Map<String, String> uniqueIndexMap,
                                          List<String> coveringGsi, List<String> gsiDbIndex,
                                          List<String> gsiPhyTableName, boolean nullable) {
        super(schemaName, logicalTableName);
        this.changeColumn = changeColumn;

        this.dbIndex = dbIndex;
        this.phyTableName = phyTableName;

        this.newColumnName = newColumnName;
        this.oldColumnName = oldColumnName;
        this.afterColumnName = afterColumnName;

        this.localIndexes = localIndexes;
        this.uniqueIndexMap = uniqueIndexMap;

        this.coveringGsi = coveringGsi;
        this.gsiDbIndex = gsiDbIndex;
        this.gsiPhyTableName = gsiPhyTableName;

        this.nullable = nullable;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        // Modify: swap 2 columns' meta, reset multi-write direction
        // Change: reset multi-write direction
        TableMetaChanger.onlineModifyColumnSwapColumn(metaDbConnection, schemaName, logicalTableName, changeColumn,
            dbIndex, phyTableName, newColumnName, oldColumnName, afterColumnName, localIndexes, uniqueIndexMap,
            coveringGsi, gsiDbIndex, gsiPhyTableName);

        // rename column for index table
        String addedColumn = changeColumn ? newColumnName : oldColumnName;
        for (String gsiName : coveringGsi) {
            updateIndexColumnMeta(metaDbConnection, schemaName, logicalTableName, gsiName, oldColumnName,
                addedColumn, GsiUtils.toNullableString(nullable));
        }

        updateSupportedCommands(true, false, metaDbConnection);
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        // TODO(qianjing): support rollback
    }

    @Override
    public String remark() {
        StringBuilder sb = new StringBuilder();
        if (changeColumn) {
            sb.append("online change column update meta on table ").append(this.getLogicalTableName());
        } else {
            sb.append("swap column ").append(this.oldColumnName);
            sb.append(" and ").append(this.newColumnName);
            sb.append(" for online modify column on table ").append(this.getLogicalTableName());
        }
        return "|" + sb;
    }

}
