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

@Getter
@TaskName(name = "RepartitionCutOverTask")
public class RepartitionCutOverTask extends BaseGmsTask {

    final String targetTableName;
    final boolean single;
    final boolean broadcast;

    public RepartitionCutOverTask(final String schemaName,
                                  final String logicalTableName,
                                  final String targetTableName,
                                  final boolean single,
                                  final boolean broadcast) {
        super(schemaName, logicalTableName);
        this.targetTableName = targetTableName;
        this.single = single;
        this.broadcast = broadcast;
        onExceptionTryRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        LOGGER.info(
            String.format("start write meta during cutOver for primary table: %s.%s", schemaName, logicalTableName)
        );
        updateSupportedCommands(true, false, metaDbConnection);
        //allowing use hint to skip clean up stage
        final String skipCutover =
            executionContext.getParamManager().getString(ConnectionParams.REPARTITION_SKIP_CUTOVER);
        if (StringUtils.equalsIgnoreCase(skipCutover, Boolean.TRUE.toString())) {
            return;
        }

        RepartitionMetaChanger.cutOver(
            metaDbConnection,
            schemaName,
            logicalTableName,
            targetTableName,
            single,
            broadcast
        );
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        LOGGER.info(
            String.format("finish write meta during cutOver for primary table: %s.%s", schemaName, logicalTableName)
        );
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

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        //暂时不支持回滚。但其实可以考虑支持回滚。
    }

}