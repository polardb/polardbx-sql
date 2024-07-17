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
import com.alibaba.polardbx.executor.ddl.job.task.columnar.CheckCciMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.CheckCciStartTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.CheckCciTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ClearCheckReportTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ShowCheckReportTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCheckCci;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CheckCciPrepareData;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.List;

/**
 * CHECK COLUMNAR INDEX
 */
public class LogicalCheckCciHandler extends LogicalCommonDdlHandler {

    public LogicalCheckCciHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext ec) {
        final LogicalCheckCci logicalCheckCci = (LogicalCheckCci) logicalDdlPlan;
        final CheckCciPrepareData prepareData = logicalCheckCci.prepareData(ec);

        // Use TransientDdlJob for CHECK COLUMNAR INDEX SHOW
        ExecutableDdlJob job = new TransientDdlJob();

        if (prepareData.isCheck()) {
            // CHECK COLUMNAR INDEX [CHECK]
            CheckCciStartTask checkCciStartTask =
                new CheckCciStartTask(prepareData.getSchemaName(), prepareData.getTableName(),
                    prepareData.getIndexName());
            CheckCciTask checkTask = CheckCciTask.create(prepareData);

            job = new ExecutableDdlJob();
            job.addSequentialTasks(ImmutableList.of(checkCciStartTask, checkTask));
        } else if (prepareData.isMeta()) {
            // CHECK COLUMNAR INDEX META
            final CheckCciMetaTask checkCciMetaTask = CheckCciMetaTask.create(prepareData);

            job = new ExecutableDdlJob();
            job.addSequentialTasks(ImmutableList.of(checkCciMetaTask));
        }

        final String fullTableName =
            DdlJobFactory.concatWithDot(prepareData.getSchemaName(), prepareData.getTableName());
        final String fullIndexName =
            DdlJobFactory.concatWithDot(prepareData.getSchemaName(), prepareData.getIndexName());
        job.addExcludeResources(Sets.newHashSet(fullTableName, fullIndexName));

        return job;
    }

    @Override
    protected Cursor buildResultCursor(BaseDdlOperation baseDdl, DdlJob ddlJob, ExecutionContext ec) {
        final LogicalCheckCci ddl = (LogicalCheckCci) baseDdl;
        final boolean async = ec.getDdlContext().isAsyncMode();

        final CheckCciPrepareData prepareData = ddl.prepareData(ec);
        final String humanReadableIndexName = prepareData.humanReadableIndexName();

        String finalResult = "";
        List<CheckerManager.CheckerReport> checkerReports = Collections.emptyList();
        if (prepareData.isClear()) {
            // Clear report from metadb
            ClearCheckReportTask clear = new ClearCheckReportTask(
                prepareData.getSchemaName(),
                prepareData.getTableName(),
                prepareData.getIndexName()
            );
            clear.clear();
            finalResult = clear.getFinalResult();
        } else if (prepareData.isNeedReport(async)) {
            // Return report as result of current CHECK COLUMNAR INDEX statement
            ShowCheckReportTask show = new ShowCheckReportTask(
                prepareData.getSchemaName(),
                prepareData.getTableName(),
                prepareData.getIndexName()
            );
            show.show(ec);
            finalResult = show.getFinalResult();
            checkerReports = show.getCheckerReports();
        } else if (!async) {
            throw new TddlRuntimeException(
                ErrorCode.ERR_DDL_JOB_ERROR,
                "unknown checker action " + prepareData.getExtraCmd());
        }

        // Generate result cursor
        ArrayResultCursor result = new ArrayResultCursor("checkColumnarIndex");
        if (async) {
            generateResultForAsyncExecution(humanReadableIndexName, result);
        } else {
            addToResultCursor(humanReadableIndexName, finalResult, checkerReports, result);
        }
        return result;
    }

    private static void generateResultForAsyncExecution(String humanReadableIndexName, ArrayResultCursor result) {
        // Add column meta
        result.addColumn("Table", DataTypes.StringType);
        result.addColumn("Op", DataTypes.StringType);
        result.addColumn("Msg_type", DataTypes.StringType);
        result.addColumn("Msg_text", DataTypes.StringType);

        // Add result
        result.addRow(new Object[] {
            humanReadableIndexName,
            "check CCI",
            "status",
            String.format(
                "Use SHOW DDL to get checking status. Use SQL: CHECK COLUMNAR INDEX %s SHOW; to get result.",
                humanReadableIndexName)});
    }

    private static void addToResultCursor(String humanReadableIndexName,
                                          String finalResult,
                                          List<CheckerManager.CheckerReport> checkerReports,
                                          ArrayResultCursor result) {
        // Add column meta
        result.addColumn("CCI", DataTypes.StringType);
        result.addColumn("error_type", DataTypes.StringType);
        result.addColumn("status", DataTypes.StringType);
        result.addColumn("primary_key", DataTypes.StringType);
        result.addColumn("details", DataTypes.StringType);

        // Add report records
        for (CheckerManager.CheckerReport item : checkerReports) {
            result.addRow(new Object[] {
                humanReadableIndexName,
                item.getErrorType(),
                CheckerManager.CheckerReportStatus.of(item.getStatus()).name(),
                item.getPrimaryKey(),
                item.getDetails()});
        }

        // Add final result
        result.addRow(new Object[] {humanReadableIndexName, "SUMMARY", "--", "--", finalResult});
    }
}
