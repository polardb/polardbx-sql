package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.executor.sync.ColumnarSnapshotUpdateSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableStatus;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@TaskName(name = "WaitColumnarTableCreationTask")
@Getter
public class WaitColumnarTableCreationTask extends BaseDdlTask {
    private static final Logger logger = LoggerFactory.getLogger(WaitColumnarTableCreationTask.class);

    private final String logicalTableName;
    private final String indexName;
    /**
     * FOR TEST USE ONLY!
     * If set to true, ddl returns succeed right after CN finish writing metadata
     */
    private final boolean skipCheck;

    public WaitColumnarTableCreationTask(String schemaName, String logicalTableName, String indexName,
                                         boolean skipCheck) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.indexName = indexName;
        this.skipCheck = skipCheck;
    }

    @Override
    @SneakyThrows
    protected void beforeTransaction(ExecutionContext executionContext) {
        // wait columnar index creation to be finished
        long start = System.nanoTime();
        while (true) {
            // Always create new metadb connection to get the latest snapshot
            try (Connection conn = Objects.requireNonNull(MetaDbDataSource.getInstance()).getConnection()) {
                TableInfoManager tableInfoManager = new TableInfoManager();
                tableInfoManager.setConnection(conn);

                List<ColumnarTableMappingRecord> records =
                    tableInfoManager.queryColumnarTable(schemaName, logicalTableName, indexName);
                if (records.isEmpty()) {
                    //找不到该列存索引记录了
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                        "Columnar table mapping record not found.");
                }
                ColumnarTableMappingRecord record = records.get(0);
                if (ColumnarTableStatus.from(record.status) == ColumnarTableStatus.PUBLIC) {
                    //创建状态成功
                    break;
                }
                //有额外信息, 意味着出现建索引错误，暂时先简单认为一定是错误
                if (record.extra != null && !record.extra.isEmpty()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, record.extra);
                }

                if (executionContext.getDdlContext().isInterrupted()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                        "The job '" + executionContext.getDdlContext().getJobId() + "' has been interrupted");
                }

                TimeUnit.MILLISECONDS.sleep(1000);
            } finally {
                SQLRecorderLogger.ddlLogger.info("Wait Columnar table created task ended, cost "
                    + ((System.nanoTime() - start) / 1_000_000) + " ms.");
            }
        }
        if (executionContext.getDdlContext().isInterrupted()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, "wait columnar table task is interrupted.");
        }
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        Long latestTso = ColumnarTransactionUtils.getLatestTsoFromGms();
        if (latestTso != null) {
            try {
                SyncManagerHelper.sync(new ColumnarSnapshotUpdateSyncAction(latestTso),
                    SystemDbHelper.DEFAULT_DB_NAME, SyncScope.ALL);
            } catch (Throwable t) {
                LOGGER.error(String.format("error occurs while updating tso after columnar index creation, tso: %d.",
                    latestTso));
                throw GeneralUtil.nestedException(t);
            }
        }
    }

    @Override
    protected boolean isSkipExecute() {
        return this.skipCheck;
    }

    @Override
    protected boolean isSkipRollback() {
        return this.skipCheck;
    }
}
