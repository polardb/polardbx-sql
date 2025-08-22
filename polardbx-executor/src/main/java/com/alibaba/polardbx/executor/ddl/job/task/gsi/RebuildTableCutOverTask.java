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

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.meta.misc.RepartitionMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * @author wumu
 */
@Getter
@TaskName(name = "RebuildTableCutOverTask")
public class RebuildTableCutOverTask extends BaseGmsTask {
    private final Map<String, String> tableNameMap;
    private final boolean autoPartition;
    private final boolean single;
    private final boolean broadcast;
    private final List<String> addColumns;
    private final List<String> dropColumns;
    private final Map<String, String> changeColumnsMap;
    private final List<String> modifyColumns;
    private final long versionId;

    public RebuildTableCutOverTask(final String schemaName,
                                   final String logicalTableName,
                                   Map<String, String> tableNameMap,
                                   boolean autoPartition,
                                   boolean single,
                                   boolean broadcast,
                                   List<String> addColumns,
                                   List<String> dropColumns,
                                   Map<String, String> changeColumnsMap,
                                   List<String> modifyColumns,
                                   long versionId) {
        super(schemaName, logicalTableName);
        this.tableNameMap = tableNameMap;
        this.autoPartition = autoPartition;
        this.single = single;
        this.broadcast = broadcast;
        this.addColumns = addColumns;
        this.dropColumns = dropColumns;
        this.changeColumnsMap = changeColumnsMap;
        this.modifyColumns = modifyColumns;
        this.versionId = versionId;
        onExceptionTryRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        LOGGER.info(
            String.format(
                "[rebuild table] start change meta during cutOver for primary table: %s.%s",
                schemaName, logicalTableName)
        );
        updateSupportedCommands(true, false, metaDbConnection);
        //allowing use hint to skip clean up stage
        final String skipCutover =
            executionContext.getParamManager().getString(ConnectionParams.REPARTITION_SKIP_CUTOVER);
        if (StringUtils.equalsIgnoreCase(skipCutover, Boolean.TRUE.toString())) {
            return;
        }

        RepartitionMetaChanger.alterTaleModifyColumnCutOver(
            metaDbConnection,
            schemaName,
            logicalTableName,
            tableNameMap,
            autoPartition,
            single,
            broadcast,
            addColumns,
            dropColumns,
            changeColumnsMap,
            modifyColumns,
            versionId,
            jobId
        );
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        LOGGER.info(
            String.format(
                "[rebuild table] finish change meta during cutOver for primary table: %s.%s",
                schemaName, logicalTableName)
        );

        EventLogger.log(EventType.DDL_INFO, "Online modify column success");
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        //sync for CutOver should keep atomic, so we won't do notify here
        //see RepartitionSyncAction.java
    }

    @Override
    protected void updateTableVersion(Connection metaDbConnection) {
        //sync for CutOver should keep atomic, so we won't do notify here
        //see RepartitionSyncAction.java
        try {
            TableInfoManager.updateTableVersion4Repartition(schemaName, logicalTableName, metaDbConnection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
