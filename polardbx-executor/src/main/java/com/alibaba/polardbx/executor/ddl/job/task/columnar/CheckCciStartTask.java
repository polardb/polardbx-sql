package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yaozhili
 */
@Getter
@TaskName(name = "CheckCciStartTask")
public class CheckCciStartTask extends CheckCciBaseTask {
    public CheckCciStartTask(String schemaName, String tableName, String indexName) {
        super(schemaName, indexName, tableName, null);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        final List<CheckerManager.CheckerReport> reports = new ArrayList<>();
        reports.add(
            createReportRecord(
                CheckCciMetaTask.ReportErrorType.SUMMARY,
                CheckerManager.CheckerReportStatus.START,
                "Start columnar index checked. job id: "));

        // Add reports to metadb.checker_reports
        CheckerManager.insertReports(metaDbConnection, reports);
    }

}
