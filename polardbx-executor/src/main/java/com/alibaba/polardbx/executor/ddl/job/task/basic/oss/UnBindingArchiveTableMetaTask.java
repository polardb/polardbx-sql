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
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.TtlTaskSqlBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TablesMetaChangePreemptiveSyncAction;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.TableLocalPartitionRecord;
import com.alibaba.polardbx.optimizer.config.table.PreemptiveTime;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.ttl.TtlUtil;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
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

    /**
     * <pre>
     *     key : tblName
     *     val :
     * </pre>
     */
    private Map<String, Integer> tableTtlTypeFlags = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
    protected final static int TTL_TYPE_FLAG_LOCAL_PARTITION = 0;
    protected final static int TTL_TYPE_FLAG_ROW_LEVEL_TTL = 1;

    @JSONCreator
    public UnBindingArchiveTableMetaTask(String schemaName, List<String> tables) {
        super(schemaName, null);
        this.tables = tables;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (tableArchive == null) {
            tableArchive = new TreeMap<>(String::compareToIgnoreCase);
        }

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        for (String table : tables) {

            TtlInfoRecord ttlInfoRecord = tableInfoManager.getTtlInfoRecord(getSchemaName(), table);
            if (ttlInfoRecord != null) {
                if (!StringUtils.isEmpty(ttlInfoRecord.getArcTblName())) {
                    tableArchive.put(table, ttlInfoRecord.getArcTblSchema() + "." + ttlInfoRecord.getArcTblName());
                    tableTtlTypeFlags.put(table, UnBindingArchiveTableMetaTask.TTL_TYPE_FLAG_ROW_LEVEL_TTL);
                    String ttlTblSchema = getSchemaName();
                    String ttlTblName = table;
                    tableInfoManager
                        .updateArchiveTableForTtlInfo(null, null, null, null, ttlTblSchema, ttlTblName);
                }
            }

            TableLocalPartitionRecord record =
                tableInfoManager.getLocalPartitionRecord(getSchemaName(), table);
            if (record.getArchiveTableName() != null) {
                tableArchive.put(table, record.getArchiveTableSchema() + "." + record.getArchiveTableName());
                tableTtlTypeFlags.put(table, UnBindingArchiveTableMetaTask.TTL_TYPE_FLAG_LOCAL_PARTITION);
                tableInfoManager
                    .updateArchiveTable(getSchemaName(), table, null, null);
            }
        }
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (tableArchive != null) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(metaDbConnection);
            for (Map.Entry<String, String> entry : tableArchive.entrySet()) {
                String tblName = entry.getKey();
                String[] tableFull = entry.getValue().split(".");
                Preconditions.checkArgument(tableFull.length == 2);

                Integer flags = tableTtlTypeFlags.get(tblName);
                if (flags != null) {
                    if (flags == UnBindingArchiveTableMetaTask.TTL_TYPE_FLAG_LOCAL_PARTITION) {
                        tableInfoManager
                            .updateArchiveTable(getSchemaName(), entry.getKey(), tableFull[0], tableFull[1]);
                    } else {
                        String ttlTblSchema = getSchemaName();
                        String ttlTblName = entry.getKey();
                        String arcTblSchema = tableFull[0];
                        String arcTblName = tableFull[1];
                        String arcTmpTblName = TtlUtil.buildArcTmpNameByArcTblName(arcTblName);
                        String arcTmpTblSchema = arcTblSchema;
                        tableInfoManager
                            .updateArchiveTableForTtlInfo(arcTblSchema, arcTblName, arcTmpTblSchema, arcTmpTblName,
                                ttlTblSchema, ttlTblName);
                    }
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
        // sync to restore the status of table meta
        PreemptiveTime preemptiveTime = PreemptiveTime.getPreemptiveTimeFromExecutionContext(executionContext,
            ConnectionParams.PREEMPTIVE_MDL_INITWAIT, ConnectionParams.PREEMPTIVE_MDL_INTERVAL);
        SyncManagerHelper.sync(
            new TablesMetaChangePreemptiveSyncAction(schemaName, tables, preemptiveTime), SyncScope.ALL);
    }

    @Override
    protected void updateTableVersion(Connection metaDbConnection) {
        try {
            for (String table : tables) {
                TableInfoManager.updateTableVersionWithoutDataId(schemaName, table, metaDbConnection);
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public String getLogicalTableName() {
        return null;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }

    public void setTableArchive(Map<String, String> tableArchive) {
        this.tableArchive = tableArchive;
    }

    @Override
    protected String remark() {
        return "|tableNames: " + Joiner.on(", ").join(tables);
    }
}
