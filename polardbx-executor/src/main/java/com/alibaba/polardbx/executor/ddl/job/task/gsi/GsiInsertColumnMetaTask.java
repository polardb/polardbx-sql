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
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangeSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

/**
 * GSI表add column时，插入GSI的index元数据。
 * <p>
 * will insert into [indexes]
 * will update [tables (version) ]
 */
@TaskName(name = "GsiInsertColumnMetaTask")
@Getter
public class GsiInsertColumnMetaTask extends BaseGmsTask {

    final String indexName;
    final List<String> columns;

    @JSONCreator
    public GsiInsertColumnMetaTask(String schemaName,
                                   String logicalTableName,
                                   String indexName,
                                   List<String> columns) {
        super(schemaName, logicalTableName);
        this.indexName = indexName;
        this.columns = ImmutableList.copyOf(columns);
        onExceptionTryRecoveryThenRollback();
    }

    /**
     * see beginAlterTableAddColumnsGsi()
     * see DdlGmsUtils.onAlterTableSuccess
     */
    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        final TableMeta primaryTableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
        final GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean =
            ExecutorContext
                .getContext(schemaName)
                .getGsiManager()
                .getGsiMetaManager()
                .getIndexMeta(schemaName, logicalTableName, indexName, IndexStatus.ALL);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        final int seqInIndex = gsiIndexMetaBean.indexColumns.size() + gsiIndexMetaBean.coveringColumns.size() + 1;

        final List<GsiMetaManager.IndexRecord> indexRecords =
            GsiUtils.buildIndexMetaByAddColumns(
                primaryTableMeta,
                Lists.newArrayList(columns),
                schemaName,
                logicalTableName,
                indexName,
                seqInIndex,
                IndexStatus.PUBLIC
            );
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        GsiMetaChanger.addIndexColumnMeta(metaDbConnection, schemaName, logicalTableName, indexRecords);

        /**
         * 新插入的indexRecord 的visible属性应该与当前gsi的record保持一致
         * indexRecord 默认插入时visible的，因此只有当前gsi是invisible的时候，才需要修正
         * */
        if (gsiIndexMetaBean.visibility == IndexVisibility.VISIBLE) {
            //do nothing
        } else if (gsiIndexMetaBean.visibility == IndexVisibility.INVISIBLE) {
            GsiMetaChanger.updateIndexVisibility(metaDbConnection, schemaName, logicalTableName, indexName,
                IndexVisibility.VISIBLE, IndexVisibility.INVISIBLE);
        }
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        for (String column : this.getColumns()) {
            ExecutorContext
                .getContext(executionContext.getSchemaName())
                .getGsiManager()
                .getGsiMetaManager()
                .removeColumnMeta(metaDbConnection, schemaName, logicalTableName, indexName, column);
            LOGGER.info(String.format("Drop GSI column cleanup task. schema:%s, table:%s, index:%s, column:%s",
                schemaName,
                logicalTableName,
                indexName,
                column));
        }

        //sync have to be successful to continue
        SyncManagerHelper.sync(new TableMetaChangeSyncAction(schemaName, logicalTableName), SyncScope.ALL);
        executionContext.refreshTableMeta();

        LOGGER.info(String.format("Rollback Change GSI meta. schema:%s, table:%s, index:%s",
            schemaName,
            logicalTableName,
            indexName
        ));
    }
}