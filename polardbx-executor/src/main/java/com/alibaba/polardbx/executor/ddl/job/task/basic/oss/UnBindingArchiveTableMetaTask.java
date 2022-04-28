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

package com.alibaba.polardbx.executor.ddl.job.task.basic.oss;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TableLocalPartitionRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.base.Joiner;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Shi Yuxuan
 */
@Getter
@TaskName(name = "UnBindingArchiveTableMetaTask")
public class UnBindingArchiveTableMetaTask extends BaseGmsTask {
    private List<String> tables;
    private Map<String, String> tableArchive;

    @JSONCreator
    public UnBindingArchiveTableMetaTask(String schemaName, List<String> tables) {
        super(schemaName, null);
        this.tables = tables;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (tableArchive != null && tableArchive.size() > 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "table archive record should be null");
        }
        tableArchive = new TreeMap<>(String::compareToIgnoreCase);

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        for (String table : tables) {
            TableLocalPartitionRecord record =
                tableInfoManager.getLocalPartitionRecord(getSchemaName(), table);
            tableArchive.put(table, record.getArchiveTableSchema() + "." + record.getArchiveTableName());
            tableInfoManager
                .updateArchiveTable(getSchemaName(), table, null, null);
        }
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (tableArchive != null) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaDbConnection);
            for (Map.Entry<String, String> entry : tableArchive.entrySet()) {
                String[] tableFull = entry.getValue().split(".");
                if (tableFull.length == 2) {
                    tableInfoManager
                        .updateArchiveTable(getSchemaName(), entry.getKey(), tableFull[0], tableFull[1]);
                }
            }
        }
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        // don't sync here, leave it to latter task
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        // don't sync here, leave it to latter task
    }

    protected void updateTableVersion(Connection metaDbConnection) {
        try {
            for (String table : tables) {
                TableInfoManager.updateTableVersion(schemaName, table, metaDbConnection);
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public String getLogicalTableName() {
        return null;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }

    public void setTableArchive(Map<String, String> tableArchive) {
        this.tableArchive = tableArchive;
    }

    protected String remark() {
        return "|tableNames: " + Joiner.on(", ").join(tables);
    }
}
