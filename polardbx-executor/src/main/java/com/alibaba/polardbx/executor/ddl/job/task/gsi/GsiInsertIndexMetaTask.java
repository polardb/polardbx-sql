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

package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.job.meta.GsiMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gms.GmsTableMetaManager;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangeSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.metadb.table.TablesExtRecord;
import com.alibaba.polardbx.gms.util.AppNameUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.ImmutableList;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * generate & insert gsiTable's metadata based on primaryTable's metadata
 * <p>
 * will insert into [indexes]
 * will update [tables_ext]
 *
 * @author guxu
 */
@TaskName(name = "GsiInsertIndexMetaTask")
@Getter
public class GsiInsertIndexMetaTask extends BaseGmsTask {

    final String indexName;
    final List<String> columns;
    final List<String> coverings;
    final boolean unique;
    final String indexComment;
    final String indexType;
    final IndexStatus indexStatus;
    Integer originTableType;
    final boolean clusteredIndex;

    @JSONCreator
    public GsiInsertIndexMetaTask(String schemaName,
                                  String logicalTableName,
                                  String indexName,
                                  List<String> columns,
                                  List<String> coverings,
                                  boolean unique,
                                  String indexComment,
                                  String indexType,
                                  IndexStatus indexStatus,
                                  boolean clusteredIndex) {
        super(schemaName, logicalTableName);
        this.indexName = indexName;
        this.columns = ImmutableList.copyOf(columns);
        this.coverings = ImmutableList.copyOf(coverings);
        this.unique = unique;
        this.indexComment = indexComment == null ? "" : indexComment;
        this.indexType = indexType;
        this.indexStatus = indexStatus;
        this.clusteredIndex = clusteredIndex;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        final List<GsiMetaManager.IndexRecord> indexRecords = new ArrayList<>();
//        final TableMeta primaryTableMeta =
//            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);

        final String appName = AppNameUtil.buildAppNameByInstAndDbName(InstIdUtil.getInstId(), schemaName);
        final TableMeta primaryTableMeta =
            GmsTableMetaManager.fetchTableMeta(metaDbConnection,
                schemaName, logicalTableName, null, null, true, true);

        FailPoint.assertNotNull(primaryTableMeta);
        primaryTableMeta.setSchemaName(schemaName);
        GsiUtils.buildIndexMetaFromPrimary(
            indexRecords,
            primaryTableMeta,
            indexName,
            columns,
            coverings,
            !unique,
            indexComment,
            indexType,
            indexStatus,
            clusteredIndex
        );

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        TablesExtRecord indexTablesExtRecord =
            tableInfoManager.queryTableExt(schemaName, indexName, false);
        if (indexTablesExtRecord != null) {
            originTableType = indexTablesExtRecord.tableType;
        }
        tableInfoManager.setConnection(null);

        //1. alter table_ext.table_type to GSI
        GsiMetaChanger.addIndexMeta(metaDbConnection, schemaName, indexRecords);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        //2. insert metadata into indexes
        GsiMetaChanger.changeTableToGsi(metaDbConnection, schemaName, indexName);

        LOGGER.info(String.format("Insert GSI meta. schema:%s, table:%s, index:%s, state:%s",
            schemaName,
            logicalTableName,
            indexName,
            indexStatus.name()
        ));
    }

    /**
     * see undoCreateGsi()
     */
    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        if (originTableType != null) {
            ExecutorContext
                .getContext(schemaName)
                .getGsiManager()
                .getGsiMetaManager()
                .changeTablesExtType(metaDbConnection, schemaName, indexName, originTableType);
        }
        GsiMetaChanger.removeIndexMeta(metaDbConnection, schemaName, logicalTableName, indexName);

        //sync have to be successful to continue
        SyncManagerHelper.sync(new TableMetaChangeSyncAction(schemaName, logicalTableName));
        executionContext.refreshTableMeta();

        LOGGER.info(String.format("Rollback Insert GSI meta. schema:%s, table:%s, index:%s, state:%s",
            schemaName,
            logicalTableName,
            indexName,
            indexStatus.name()
        ));
    }
}
