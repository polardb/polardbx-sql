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

import com.alibaba.polardbx.executor.ddl.job.meta.CommonMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;

@Getter
@TaskName(name = "OnlineModifyColumnDropMetaTask")
public class OnlineModifyColumnDropMetaTask extends BaseGmsTask {
    private final boolean changeColumn;

    private final String newColumnName;
    private final String oldColumnName;

    private final String dbIndex;
    private final String phyTableName;

    private final List<String> coveringGsi;
    private final List<String> gsiDbIndex;
    private final List<String> gsiPhyTableName;

    private final boolean unique;

    public OnlineModifyColumnDropMetaTask(String schemaName, String logicalTableName, boolean changeColumn,
                                          String dbIndex, String phyTableName, String newColumnName,
                                          String oldColumnName, List<String> coveringGsi,
                                          List<String> gsiDbIndex, List<String> gsiPhyTableName, boolean unique) {
        super(schemaName, logicalTableName);
        this.changeColumn = changeColumn;

        this.dbIndex = dbIndex;
        this.phyTableName = phyTableName;

        this.newColumnName = newColumnName;
        this.oldColumnName = oldColumnName;

        this.coveringGsi = coveringGsi;
        this.gsiDbIndex = gsiDbIndex;
        this.gsiPhyTableName = gsiPhyTableName;

        this.unique = unique;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        String droppedColumn = changeColumn ? oldColumnName : newColumnName;
        String keptColumn = changeColumn ? newColumnName : oldColumnName;
        TableMetaChanger.onlineModifyColumnDropColumn(metaDbConnection, schemaName, logicalTableName, dbIndex,
            phyTableName, droppedColumn, coveringGsi, gsiDbIndex, gsiPhyTableName, unique, keptColumn);

        CommonMetaChanger.alterTableColumnFinalOperationsOnSuccess(schemaName, logicalTableName,
            Collections.singletonList(oldColumnName));
    }

    @Override
    public String remark() {
        StringBuilder sb = new StringBuilder();
        if (changeColumn) {
            sb.append("drop column ").append(this.oldColumnName);
            sb.append(" for online change column on table ").append(this.getLogicalTableName());
        } else {
            sb.append("drop column ").append(this.newColumnName);
            sb.append(" for online modify column on table ").append(this.getLogicalTableName());
        }
        return "|" + sb;
    }

}
