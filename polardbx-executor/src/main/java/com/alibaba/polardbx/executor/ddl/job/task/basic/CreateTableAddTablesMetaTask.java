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

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager.PhyInfoSchemaContext;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

@Getter
@TaskName(name = "CreateTableAddTablesMetaTask")
public class CreateTableAddTablesMetaTask extends BaseGmsTask {

    private String dbIndex;
    private String phyTableName;
    private SequenceBean sequenceBean;
    private TablesExtRecord tablesExtRecord;
    private boolean partitioned;
    private boolean ifNotExists;
    private SqlKind sqlKind;
    private boolean hasTimestampColumnDefault;
    private List<ForeignKeyData> addedForeignKeys;
    private Map<String, String> specialDefaultValues;
    private Map<String, Long> specialDefaultValueFlags;
    private Map<String, String> columnMapping;
    private List<String> addNewColumns;

    @JSONCreator
    public CreateTableAddTablesMetaTask(String schemaName, String logicalTableName, String dbIndex, String phyTableName,
                                        SequenceBean sequenceBean, TablesExtRecord tablesExtRecord,
                                        boolean partitioned, boolean ifNotExists, SqlKind sqlKind,
                                        List<ForeignKeyData> addedForeignKeys,
                                        boolean hasTimestampColumnDefault,
                                        Map<String, String> specialDefaultValues,
                                        Map<String, Long> specialDefaultValueFlags,
                                        Map<String, String> columnMapping,
                                        List<String> addNewColumns) {
        super(schemaName, logicalTableName);
        this.dbIndex = dbIndex;
        this.phyTableName = phyTableName;
        this.sequenceBean = sequenceBean;
        this.tablesExtRecord = tablesExtRecord;
        this.partitioned = partitioned;
        this.ifNotExists = ifNotExists;
        this.sqlKind = sqlKind;
        this.addedForeignKeys = addedForeignKeys;
        this.hasTimestampColumnDefault = hasTimestampColumnDefault;
        this.specialDefaultValues = specialDefaultValues;
        this.specialDefaultValueFlags = specialDefaultValueFlags;
        this.columnMapping = columnMapping;
        this.addNewColumns = addNewColumns;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        PhyInfoSchemaContext phyInfoSchemaContext = TableMetaChanger.buildPhyInfoSchemaContext(schemaName,
            logicalTableName, dbIndex, phyTableName, sequenceBean, tablesExtRecord, partitioned, ifNotExists, sqlKind,
            0L, executionContext);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        TableMetaChanger.addTableMeta(metaDbConnection, phyInfoSchemaContext, hasTimestampColumnDefault,
            executionContext, specialDefaultValues, specialDefaultValueFlags, addedForeignKeys, columnMapping,
            addNewColumns);
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.removeTableMeta(metaDbConnection, schemaName, logicalTableName, false, executionContext);
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        TableMetaChanger.afterRemovingTableMeta(schemaName, logicalTableName);
    }
}
