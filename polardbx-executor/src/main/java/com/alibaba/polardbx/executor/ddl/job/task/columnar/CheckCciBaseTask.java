package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import lombok.Getter;
import org.apache.calcite.sql.SqlCheckColumnarIndex;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Optional;

/**
 * @author yaozhili
 */
@Getter
public abstract class CheckCciBaseTask extends BaseDdlTask {
    /**
     * Blank if {@link #indexName} not exists in {@link #schemaName}
     */
    protected final String tableName;
    protected final String indexName;
    protected final SqlCheckColumnarIndex.CheckCciExtraCmd extraCmd;

    @JSONCreator
    public CheckCciBaseTask(String schemaName, String tableName, String indexName,
                            SqlCheckColumnarIndex.CheckCciExtraCmd extraCmd) {
        super(schemaName);
        this.tableName = tableName;
        this.indexName = indexName;
        this.extraCmd = extraCmd;
    }

    protected CheckerManager.CheckerReport createReportRecord(CheckCciMetaTask.ReportErrorType errorType,
                                                              CheckerManager.CheckerReportStatus status,
                                                              String detail) {
        return createReportRecord(errorType, status, "--", detail, "Reporter.");
    }

    protected CheckerManager.CheckerReport createReportRecord(CheckCciMetaTask.ReportErrorType errorType,
                                                              CheckerManager.CheckerReportStatus status,
                                                              String primaryKey,
                                                              String detail,
                                                              String extra) {
        return new CheckerManager.CheckerReport(-1,
            this.jobId,
            this.schemaName,
            this.tableName,
            this.schemaName,
            this.indexName,
            "",
            "",
            Optional
                .ofNullable(errorType)
                .filter(e -> e != CheckCciMetaTask.ReportErrorType.UNKNOWN)
                .map(CheckCciMetaTask.ReportErrorType::name)
                .orElse("--"),
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime()),
            status.getValue(),
            primaryKey,
            detail,
            extra,
            null
        );
    }

}
