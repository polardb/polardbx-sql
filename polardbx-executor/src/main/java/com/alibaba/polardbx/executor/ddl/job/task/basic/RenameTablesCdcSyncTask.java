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

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.cdc.TablesExtInfo;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.sync.LockTablesSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TablesMetaChangePreemptiveSyncAction;
import com.alibaba.polardbx.executor.sync.UnlockTableSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.config.table.PreemptiveTime;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil.buildExtendParameter;

/**
 * @author wumu
 */
@Getter
@TaskName(name = "RenameTablesCdcSyncTask")
public class RenameTablesCdcSyncTask extends TablesSyncTask {
    private List<String> oldTableNames;
    private List<String> newTableNames;

    private List<String> collates;
    private List<TablesExtInfo> cdcMetas;
    private List<Map<String, Set<String>>> newTableTopologies;
    private final List<Long> versionIds;

    public RenameTablesCdcSyncTask(String schemaName,
                                   List<String> tableNames,
                                   boolean preemptive,
                                   PreemptiveTime preemptiveTime,
                                   List<String> oldTableNames,
                                   List<String> newTableNames,
                                   List<String> collates,
                                   List<TablesExtInfo> cdcMetas,
                                   List<Map<String, Set<String>>> newTableTopologies,
                                   List<Long> versionIds
    ) {
        super(schemaName, tableNames, preemptive, preemptiveTime);
        this.oldTableNames = oldTableNames;
        this.newTableNames = newTableNames;
        this.collates = collates;
        this.cdcMetas = cdcMetas;
        this.newTableTopologies = newTableTopologies;
        this.versionIds = versionIds;
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        updateSupportedCommands(true, false, null);
        executeImpl(executionContext);
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        GsiValidator.validateEnableMDL(executionContext);
        long uniqueConnId = executionContext.getConnId() + Long.MAX_VALUE;
        // lock tables
        try {
            LOGGER.info(
                String.format("start lock table during rename table for tables: %s.%s", schemaName, tableNames)
            );
            FailPoint.injectRandomExceptionFromHint(executionContext);
            FailPoint.injectRandomSuspendFromHint(executionContext);
            // Sync will reload and clear cross status transaction.
            SyncManagerHelper.sync(
                new LockTablesSyncAction(schemaName,
                    tableNames,
                    executionContext.getTraceId(),
                    uniqueConnId,
                    preemptiveTime
                ),
                schemaName,
                SyncScope.ALL,
                true
            );

            LOGGER.info(
                String.format("finish lock table during rename table for tables: %s.%s", schemaName, tableNames)
            );

        } catch (Exception e) {
            String errMsg = String.format(
                "error occurs while lock table during rename table, tableName:%s", tableNames
            );
            LOGGER.error(errMsg);
            throw GeneralUtil.nestedException(e);
        }

        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        // cdc mark
        DdlContext ddlContext = executionContext.getDdlContext();
        for (int i = 0; i < oldTableNames.size(); ++i) {
            String tableName = oldTableNames.get(i);
            String newTableName = newTableNames.get(i);

            String ddlSql = String.format("rename table %s to %s", tableName, newTableName);

            Map<String, Object> params = buildExtendParameter(executionContext);
            params.put(ICdcManager.TABLE_NEW_NAME, newTableName);
            params.put(ICdcManager.TASK_MARK_SEQ, i + 1);

            CdcManagerHelper.getInstance()
                .notifyDdlNew(schemaName, tableName, "RENAME_TABLE",
                    getDdlStmt(ddlSql, versionIds.get(i)), ddlContext.getDdlType(), ddlContext.getJobId(), getTaskId(),
                    CdcDdlMarkVisibility.Public, params, true, newTableTopologies.get(i),
                    new Pair<>(collates.get(i), cdcMetas.get(i)));
        }

        // road table meta
        try {
            SyncManagerHelper.sync(
                new TablesMetaChangePreemptiveSyncAction(schemaName, tableNames, preemptiveTime,
                    uniqueConnId, false),
                SyncScope.ALL,
                true);
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while sync table meta, schemaName:%s, tableNames:%s", schemaName, tableNames.toString()));
            throw GeneralUtil.nestedException(t);
        }
    }

    @Override
    public void handleError(ExecutionContext executionContext) {
        long uniqueConnId = executionContext.getConnId() + Long.MAX_VALUE;
        try {
            LOGGER.info(
                String.format("start unlock table during rename table for tables: %s.%s", schemaName, tableNames)
            );
            FailPoint.injectRandomExceptionFromHint(executionContext);
            FailPoint.injectRandomSuspendFromHint(executionContext);
            // Sync will reload and clear cross status transaction.
            SyncManagerHelper.sync(
                new UnlockTableSyncAction(schemaName,
                    tableNames.get(0),
                    uniqueConnId,
                    executionContext.getTraceId()
                ),
                schemaName,
                SyncScope.ALL,
                true
            );

            LOGGER.info(
                String.format("finish unlock table during rename table for tables: %s.%s", schemaName, tableNames)
            );

        } catch (Exception e) {
            String errMsg = String.format(
                "error occurs while unlock table during rename table, tableName:%s", tableNames
            );
            LOGGER.error(errMsg);
            throw GeneralUtil.nestedException(e);
        }
    }

    private String getDdlStmt(String ddl, Long versionId) {
        if (CdcMarkUtil.isVersionIdInitialized(versionId)) {
            return CdcMarkUtil.buildVersionIdHint(versionId) + ddl;
        }
        return ddl;
    }
}
