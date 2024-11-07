package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.columnar.checker.CciIncrementalChecker;
import com.alibaba.polardbx.executor.columnar.checker.ICciChecker;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CheckCciPrepareData;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;
import org.apache.calcite.sql.SqlCheckColumnarIndex;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yaozhili
 */
@Getter
@TaskName(name = "CheckCciIncrementTask")
public class CheckCciIncrementTask extends CheckCciBaseTask {
    private final static Logger LOG = LoggerFactory.getLogger(CheckCciIncrementTask.class);

    final List<CheckerManager.CheckerReport> reports = new ArrayList<>();
    long tsoV0;
    long tsoV1;
    long innodbTso;

    public static CheckCciIncrementTask create(CheckCciPrepareData prepareData) {
        return new CheckCciIncrementTask(
            prepareData.getSchemaName(),
            prepareData.getTableName(),
            prepareData.getIndexName(),
            prepareData.getExtraCmd(),
            prepareData.getTsoList().get(0),
            prepareData.getTsoList().get(1),
            prepareData.getTsoList().get(2)
        );
    }

    @JSONCreator
    public CheckCciIncrementTask(String schemaName,
                                 String tableName,
                                 String indexName,
                                 SqlCheckColumnarIndex.CheckCciExtraCmd extraCmd,
                                 long tsoV0,
                                 long tsoV1,
                                 long innodbTso) {
        super(schemaName, tableName, indexName, extraCmd);
        this.tsoV0 = tsoV0;
        this.tsoV1 = tsoV1;
        this.innodbTso = innodbTso;
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        // Check.
        ICciChecker checker = new CciIncrementalChecker(schemaName, tableName, indexName);
        doCheck(executionContext, checker);
    }

    protected void doCheck(ExecutionContext executionContext, ICciChecker checker) {
        long startTime = System.nanoTime();
        try {
            checker.check(executionContext, tsoV0, tsoV1, innodbTso);
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
        } else if (!checkReports.isEmpty()) {
            for (String report : checkReports) {
                reports.add(createReportRecord(CheckCciMetaTask.ReportErrorType.SUMMARY,
                    CheckerManager.CheckerReportStatus.FINISH, report));
            }
        }

        reports.add(
            createReportRecord(
                CheckCciMetaTask.ReportErrorType.SUMMARY,
                CheckerManager.CheckerReportStatus.FINISH,
                "incremental data of columnar index between " + tsoV0 + " and " + tsoV1 + " checked."
                    + "Cost " + (System.nanoTime() - startTime) / 1000000 + "ms."
            ));
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        // Add reports to metadb.checker_reports
        CheckerManager.insertReports(metaDbConnection, reports);
    }

    @Override
    public String remark() {
        return String.format("|CciIncrementCheck(%s.%s)", tableName, indexName);
    }
}
