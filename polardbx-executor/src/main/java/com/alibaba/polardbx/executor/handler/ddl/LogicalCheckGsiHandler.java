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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.CheckGsiTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ClearCheckReportTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ShowCheckReportTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateGsiExistenceTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCheckGsi;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CheckGsiPrepareData;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * CHECK GLOBAL INDEX
 *
 * @author moyi
 * @since 2021/07
 */
public class LogicalCheckGsiHandler extends LogicalCommonDdlHandler {

    public LogicalCheckGsiHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext ec) {
        LogicalCheckGsi logicalCheckGsi = (LogicalCheckGsi) logicalDdlPlan;
        CheckGsiPrepareData prepareData = logicalCheckGsi.prepareData(ec);
        ExecutableDdlJob job = new ExecutableDdlJob();

        if (prepareData.isCheck() || prepareData.isCorrect()) {
            // just validate existence, but not drop
            ValidateGsiExistenceTask validateTask = new ValidateGsiExistenceTask(
                prepareData.getSchemaName(),
                prepareData.getTableName(),
                prepareData.getIndexName()
            );
            CheckGsiTask checkTask = CheckGsiTask.create(prepareData);
            job.addSequentialTasks(Arrays.asList(validateTask, checkTask));
            job.labelAsHead(validateTask);
            job.labelAsTail(checkTask);
        }

        return job;
    }

    @Override
    protected Cursor buildResultCursor(BaseDdlOperation baseDdl, ExecutionContext ec) {
        LogicalCheckGsi ddl = (LogicalCheckGsi) baseDdl;
        boolean async = ec.getDdlContext().isAsyncMode();

        CheckGsiPrepareData prepareData = ddl.prepareData(ec);
        String prettyTableName = prepareData.prettyTableName();
        String finalResult;
        List<CheckerManager.CheckerReport> checkerReports = Collections.emptyList();

        // query result from metadb
        boolean needReport = prepareData.isShow() || (!async && prepareData.isCheck() || prepareData.isCorrect());
        if (prepareData.isClear()) {
            ClearCheckReportTask clear = new ClearCheckReportTask(
                prepareData.getSchemaName(),
                prepareData.getTableName(),
                prepareData.getIndexName()
            );
            clear.clear();
            finalResult = clear.getFinalResult();
        } else if (needReport) {
            ShowCheckReportTask show = new ShowCheckReportTask(
                prepareData.getSchemaName(),
                prepareData.getTableName(),
                prepareData.getIndexName()
            );
            show.show(ec);
            finalResult = show.getFinalResult();
            checkerReports = show.getCheckerReports();
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "unknown checker action");
        }

        // generate a cursor
        ArrayResultCursor result = new ArrayResultCursor("checkGlobalIndex");
        if (async) {
            result.addColumn("Table", DataTypes.StringType);
            result.addColumn("Op", DataTypes.StringType);
            result.addColumn("Msg_type", DataTypes.StringType);
            result.addColumn("Msg_text", DataTypes.StringType);

            final String friendlyTableName = prepareData.prettyTableName();
            result.addRow(new Object[] {
                friendlyTableName, "check GSI", "status",
                "Use SHOW DDL to get checking status. Use SQL: { CHECK GLOBAL INDEX "
                    + friendlyTableName
                    + " SHOW; } to get result."});
        } else {
            result.addColumn("GSI_table", DataTypes.StringType);
            result.addColumn("error_type", DataTypes.StringType);
            result.addColumn("status", DataTypes.StringType);
            result.addColumn("primary_key", DataTypes.StringType);
            result.addColumn("details", DataTypes.StringType);

            for (CheckerManager.CheckerReport item : checkerReports) {
                result.addRow(new Object[] {
                    prettyTableName, item.getErrorType(),
                    CheckerManager.CheckerReportStatus.of(item.getStatus()).name(),
                    item.getPrimaryKey(), item.getDetails()});
            }
            // Add final result.
            result.addRow(new Object[] {prettyTableName, "SUMMARY", "--", "--", finalResult});
        }
        return result;
    }

}
