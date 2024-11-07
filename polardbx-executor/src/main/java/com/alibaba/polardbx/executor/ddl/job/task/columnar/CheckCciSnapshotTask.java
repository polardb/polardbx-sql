package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.columnar.checker.CciSnapshotChecker;
import com.alibaba.polardbx.executor.columnar.checker.CciSnapshotFastChecker;
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

@Getter
@TaskName(name = "CheckCciSnapshotTask")
public class CheckCciSnapshotTask extends CheckCciBaseTask {
    private final static Logger LOG = LoggerFactory.getLogger(CheckCciTask.class);

    final List<CheckerManager.CheckerReport> reports = new ArrayList<>();
    private final long primaryTso;
    private final long columnarTso;

    public static CheckCciSnapshotTask create(CheckCciPrepareData prepareData) {
        return new CheckCciSnapshotTask(
            prepareData.getSchemaName(),
            prepareData.getTableName(),
            prepareData.getIndexName(),
            prepareData.getExtraCmd(),
            prepareData.getTsoList().get(0),
            prepareData.getTsoList().get(1)
        );
    }

    @JSONCreator
    public CheckCciSnapshotTask(String schemaName,
                                String tableName,
                                String indexName,
                                SqlCheckColumnarIndex.CheckCciExtraCmd extraCmd,
                                long primaryTso,
                                long columnarTso) {
        super(schemaName, tableName, indexName, extraCmd);
        this.primaryTso = primaryTso;
        this.columnarTso = columnarTso;
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        // Check.
        ICciChecker checker;
        if (executionContext.isEnableCciFastChecker() && ExecUtils.canUseCciFastChecker(schemaName, indexName)) {
            checker = new CciSnapshotFastChecker(schemaName, tableName, indexName, primaryTso, columnarTso);
        } else {
            checker = new CciSnapshotChecker(schemaName, tableName, indexName, primaryTso, columnarTso);
        }

        doCheck(executionContext, checker);
    }

    protected void doCheck(ExecutionContext executionContext, ICciChecker checker) {
        try {
            checker.check(executionContext);
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
                "data of columnar index checked"));
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        // Add reports to metadb.checker_reports
        CheckerManager.insertReports(metaDbConnection, reports);
    }

    @Override
    public String remark() {
        return String.format("|CciSnapshotCheck(%s.%s)", tableName, indexName);
    }
}
