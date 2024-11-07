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

package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.columnar.checker.CciChecker;
import com.alibaba.polardbx.executor.columnar.checker.CciFastChecker;
import com.alibaba.polardbx.executor.columnar.checker.ICciChecker;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CheckCciPrepareData;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;
import org.apache.calcite.sql.SqlCheckColumnarIndex;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * Check consistency of clustered columnar index
 *
 * @author yaozhili
 */
@Getter
@TaskName(name = "CheckCciTask")
public class CheckCciTask extends CheckCciBaseTask {

    private final static Logger LOG = LoggerFactory.getLogger(CheckCciTask.class);

    final List<CheckerManager.CheckerReport> reports = new ArrayList<>();

    public static CheckCciTask create(CheckCciPrepareData prepareData) {
        return new CheckCciTask(
            prepareData.getSchemaName(),
            prepareData.getTableName(),
            prepareData.getIndexName(),
            prepareData.getExtraCmd()
        );
    }

    @JSONCreator
    public CheckCciTask(String schemaName,
                        String tableName,
                        String indexName,
                        SqlCheckColumnarIndex.CheckCciExtraCmd extraCmd) {
        super(schemaName, tableName, indexName, extraCmd);
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        // Check.
        long startTime = System.nanoTime();
        ICciChecker checker;
        if (executionContext.isEnableCciFastChecker() && ExecUtils.canUseCciFastChecker(schemaName, indexName)) {
            checker = new CciFastChecker(schemaName, tableName, indexName);
        } else {
            checker = new CciChecker(schemaName, tableName, indexName);
        }

        Runnable recover = null;
        if (executionContext.isForce2pcDuringCciCheck()) {
            try {
                recover = ExecUtils.forceAllTrx2PC();
            } catch (Throwable t) {
                recover = null;
            }
        }

        try {
            checker.check(executionContext, recover);
        } catch (Throwable t) {
            reports.add(
                createReportRecord(
                    CheckCciMetaTask.ReportErrorType.SUMMARY,
                    CheckerManager.CheckerReportStatus.FOUND,
                    "Error occurs when checking, caused by " + t.getMessage()
                ));
            SQLRecorderLogger.ddlLogger.error(t);
        }

        List<String> checkReports = new ArrayList<>();
        if (!checker.getCheckReports(checkReports)) {
            // Inconsistency detected.
            for (String error : checkReports) {
                reports.add(createReportRecord(CheckCciMetaTask.ReportErrorType.SUMMARY,
                    CheckerManager.CheckerReportStatus.FOUND, error));
            }
        }

        reports.add(
            createReportRecord(
                CheckCciMetaTask.ReportErrorType.SUMMARY,
                CheckerManager.CheckerReportStatus.FINISH,
                "data of columnar index checked."
                    + "Cost " + (System.nanoTime() - startTime) / 1000000 + "ms"
            ));
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        // Add reports to metadb.checker_reports
        CheckerManager.insertReports(metaDbConnection, reports);
    }

    @Override
    public String remark() {
        return String.format("|CciCheck(%s.%s)", tableName, indexName);
    }
}
