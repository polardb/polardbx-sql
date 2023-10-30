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
import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;
import lombok.Getter;
import org.apache.calcite.sql.SequenceBean;
import org.apache.calcite.sql.SqlKind;

import java.sql.Connection;

@Getter
@TaskName(name = "CreateOssTableAddTablesMetaTask")
public class CreateOssTableAddTablesMetaTask extends BaseGmsTask {
    private String dbIndex;
    private String phyTableName;
    private SequenceBean sequenceBean;
    private TablesExtRecord tablesExtRecord;
    private boolean partitioned;
    private boolean ifNotExists;
    private SqlKind sqlKind;
    private Engine tableEngine;

    @JSONCreator
    public CreateOssTableAddTablesMetaTask(String schemaName, String logicalTableName, String dbIndex,
                                           String phyTableName,
                                           SequenceBean sequenceBean, TablesExtRecord tablesExtRecord,
                                           boolean partitioned, boolean ifNotExists, SqlKind sqlKind,
                                           Engine tableEngine) {
        super(schemaName, logicalTableName);
        this.dbIndex = dbIndex;
        this.phyTableName = phyTableName;
        this.sequenceBean = sequenceBean;
        this.tablesExtRecord = tablesExtRecord;
        this.partitioned = partitioned;
        this.ifNotExists = ifNotExists;
        this.sqlKind = sqlKind;
        this.tableEngine = tableEngine;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        final ITimestampOracle timestampOracle =
            executionContext.getTransaction().getTransactionManagerUtil().getTimestampOracle();
        if (null == timestampOracle) {
            throw new UnsupportedOperationException("Do not support timestamp oracle");
        }
        long ts = timestampOracle.nextTimestamp();

        TableInfoManager.PhyInfoSchemaContext phyInfoSchemaContext =
            TableMetaChanger.buildPhyInfoSchemaContext(schemaName, logicalTableName, dbIndex, phyTableName,
                sequenceBean, tablesExtRecord, partitioned, ifNotExists, sqlKind, ts, executionContext);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        TableMetaChanger.addOssTableMeta(metaDbConnection, phyInfoSchemaContext, tableEngine, executionContext);
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
