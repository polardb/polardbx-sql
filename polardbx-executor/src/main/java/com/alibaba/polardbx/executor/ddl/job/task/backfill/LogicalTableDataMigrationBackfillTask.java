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

package com.alibaba.polardbx.executor.ddl.job.task.backfill;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.CostEstimableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.RemoteExecutableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.migration.TableMigrationTaskInfo;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalTableDataMigrationBackfill;
import lombok.Getter;

import java.sql.Connection;

/**
 * Created by zhuqiwei.
 */
@TaskName(name = "LogicalTableDataMigrationBackfillTask")
@Getter
public class LogicalTableDataMigrationBackfillTask extends BaseBackfillTask
    implements RemoteExecutableDdlTask, CostEstimableDdlTask {
    String srcSchemaName;
    String dstSchemaName;
    String srcTableName;
    String dstTableName;

    private transient volatile CostInfo costInfo;
    boolean backFillErrorHappened;
    String errorMessage;

    @JSONCreator
    public LogicalTableDataMigrationBackfillTask(String srcSchemaName,
                                                 String dstSchemaName,
                                                 String srcTableName,
                                                 String dstTableName) {
        super(dstSchemaName);
        this.srcSchemaName = srcSchemaName;
        this.dstSchemaName = dstSchemaName;
        this.srcTableName = srcTableName;
        this.dstTableName = dstTableName;
        this.backFillErrorHappened = false;
        onExceptionTryRecoveryThenRollback();
    }

    protected void executeImpl(ExecutionContext executionContext) {
        try {
            executionContext = executionContext.copy();
            executionContext.setBackfillId(getTaskId());
            executionContext.setSchemaName(schemaName);
            LogicalTableDataMigrationBackfill logicalTableDataMigrationBackfill =
                LogicalTableDataMigrationBackfill.createLogicalTableDataMigrationBackfill(srcSchemaName, dstSchemaName,
                    srcTableName, dstTableName, executionContext);
            FailPoint.injectRandomExceptionFromHint(executionContext);
            FailPoint.injectRandomSuspendFromHint(executionContext);
            ExecutorHelper.execute(logicalTableDataMigrationBackfill, executionContext);
        } catch (Throwable e) {
            errorMessage = e.getMessage();
            backFillErrorHappened = true;
        }
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        executeImpl(executionContext);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        //log the execution result to 'extra field'
        DdlEngineTaskAccessor ddlEngineTaskAccessor = new DdlEngineTaskAccessor();
        ddlEngineTaskAccessor.setConnection(metaDbConnection);
        TableMigrationTaskInfo extraInfo;
        if (backFillErrorHappened) {
            extraInfo = new TableMigrationTaskInfo(
                srcSchemaName,
                dstSchemaName,
                dstTableName,
                false,
                errorMessage,
                null,
                null
            );
        } else {
            extraInfo = new TableMigrationTaskInfo(
                srcSchemaName,
                dstSchemaName,
                dstTableName,
                true,
                null,
                null,
                null
            );
        }

        ddlEngineTaskAccessor.updateExtraInfoForCreateDbAsLike(
            jobId,
            taskId,
            JSON.toJSONString(extraInfo)
        );
    }

    public String getSrcSchemaName() {
        return srcSchemaName;
    }

    public void setSrcSchemaName(String srcSchemaName) {
        this.srcSchemaName = srcSchemaName;
    }

    public String getDstSchemaName() {
        return dstSchemaName;
    }

    public void setDstSchemaName(String dstSchemaName) {
        this.dstSchemaName = dstSchemaName;
    }

    public String getSrcTableName() {
        return srcTableName;
    }

    public void setSrcTableName(String srcTableName) {
        this.srcTableName = srcTableName;
    }

    public String getDstTableName() {
        return dstTableName;
    }

    public void setDstTableName(String dstTableName) {
        this.dstTableName = dstTableName;
    }

    public boolean isBackFillErrorHappened() {
        return backFillErrorHappened;
    }

    public void setBackFillErrorHappened(boolean backFillErrorHappened) {
        this.backFillErrorHappened = backFillErrorHappened;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public void setCostInfo(CostInfo costInfo) {
        this.costInfo = costInfo;
    }

    @Override
    public CostInfo getCostInfo() {
        return costInfo;
    }

    @Override
    public String remark() {
        return String.format("|Backfill from schema[%s]-table[%s] to schema[%s]-table[%s]", srcSchemaName, srcTableName,
            dstSchemaName, dstTableName);
    }
}
