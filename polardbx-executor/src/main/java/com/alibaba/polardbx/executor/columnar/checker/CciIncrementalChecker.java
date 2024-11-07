package com.alibaba.polardbx.executor.columnar.checker;

import com.alibaba.polardbx.common.IInnerConnection;
import com.alibaba.polardbx.common.IInnerConnectionManager;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.oss.ColumnarFileType;
import com.alibaba.polardbx.common.oss.IDeltaReadOption;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.mpp.split.SpecifiedOssSplit;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.metadb.table.FilesRecordSimplified;
import com.alibaba.polardbx.gms.metadb.table.FilesRecordSimplifiedWithChecksum;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

/**
 * @author yaozhili
 */
public class CciIncrementalChecker implements ICciChecker {
    private static final Logger logger = LoggerFactory.getLogger(CciIncrementalChecker.class);
    private final String schemaName;
    private final String tableName;
    private final String indexName;
    private long finalCount;
    private final List<String> errors = new ArrayList<>();
    /**
     * Record connection id in use.
     * If the checking thread is interrupted, kill these connections.
     */
    private final Set<IInnerConnection> connections = new ConcurrentSkipListSet<>(
        (o1, o2) -> {
            if (o1 == null && o2 == null) {
                return 0;
            }
            if (o1 == null) {
                return -1;
            }
            if (o2 == null) {
                return 1;
            }
            return o1.hashCode() - o2.hashCode();
        }
    );

    private static final String COLUMNAR_HINT =
        "/*+TDDL:WORKLOAD_TYPE=AP ENABLE_MPP=true ENABLE_MASTER_MPP=true ENABLE_COLUMNAR_OPTIMIZER=true "
            + "OPTIMIZER_TYPE='columnar' ENABLE_HTAP=true ENABLE_BLOCK_CACHE=false CCI_INCREMENTAL_CHECK=true "
            + "SOCKET_TIMEOUT=259200000 MPP_TASK_MAX_RUN_TIME=259200000 %s */";

    public CciIncrementalChecker(String schemaName, String tableName, String indexName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.indexName = indexName;
    }

    @Override
    public void check(ExecutionContext ec, long tsoV0, long tsoV1, long innodbTso) throws Throwable {
        SQLRecorderLogger.ddlLogger.info("[Incremental checker] Check cci increment for " + schemaName + "."
            + indexName + " " + tsoV0 + " " + tsoV1 + " " + innodbTso);
        long begin = System.nanoTime();
        ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        IInnerConnectionManager connectionManager = executorContext.getInnerConnectionManager();
        try (IInnerConnection conn = connectionManager.getConnection(schemaName)) {
            connections.add(conn);

            ResultSet rs = getIncrementalInsertData(ec, tsoV0, tsoV1, conn);
            if (null == rs || !rs.next()) {
                return;
            }

            AtomicBoolean finish = new AtomicBoolean(false);
            int parallelism = DynamicConfig.getInstance().getCciIncrementalCheckParallelism();
            int columnCount = rs.getMetaData().getColumnCount();
            final List<String> columnNames = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                columnNames.add(rs.getMetaData().getColumnName(i));
            }
            String concatColumnName = "(" + String.join(",", columnNames) + ")";
            int batchSize = DynamicConfig.getInstance().getCciIncrementalCheckBatchSize();
            // Each task like: ((0, 0, 0), (1, 1, 1), (2, 2, 2))
            ConcurrentLinkedQueue<String> checkInsertValues = new ConcurrentLinkedQueue<>();
            List<Future<Throwable>> futures = new ArrayList<>(parallelism);
            ServerThreadPool threadPool = executorContext.getTopologyExecutor().getExecutorService();
            AtomicLong count = new AtomicLong(0);
            // Consumers.
            addConsumers(innodbTso, connectionManager, finish, parallelism, concatColumnName, checkInsertValues,
                futures,
                threadPool, count);
            Throwable exception = null;
            // Producer.
            int expectedCount = 0;
            try {
                // Already rs.next(), just get the result row.
                do {
                    List<String> values = new ArrayList<>(batchSize);
                    int currentBatchSize = 0;
                    do {
                        expectedCount++;
                        List<String> value = new ArrayList<>(columnCount);
                        for (int i = 0; i < columnCount; i++) {
                            value.add("'" + rs.getString(columnNames.get(i)) + "'");
                        }
                        values.add("(" + String.join(",", value) + ")");
                        currentBatchSize++;
                        if (DynamicConfig.getInstance().isEnableColumnarDebug()) {
                            logger.info("Incremental check found value: "
                                + "(" + String.join(",", value) + ")");
                        }
                    } while (currentBatchSize < batchSize && rs.next());
                    checkInsertValues.add("(" + String.join(",", values) + ")");
                } while (rs.next());
            } catch (Throwable t) {
                exception = t;
            } finally {
                finish.set(true);
            }

            checkException(futures, exception);

            // Wait all tasks.
            exception = waitAllTasks(ec, futures);

            checkException(futures, exception);

            if (expectedCount != count.get()) {
                this.errors.add("Incremental check failed, expected columnar count: " + expectedCount
                    + ", actual row-store count: " + count.get());
            } else {
                this.finalCount = count.get();
            }
        } finally {
            connections.clear();
            SQLRecorderLogger.ddlLogger.info(
                "[Incremental checker] Total cost: " + (System.nanoTime() - begin) / 1_000_000 + " ms");
        }
    }

    private Throwable waitAllTasks(ExecutionContext ec, List<Future<Throwable>> futures)
        throws Throwable {
        Throwable exception = null;
        while (true) {
            boolean done = true;
            for (Future<Throwable> future : futures) {
                try {
                    Throwable t = future.get(1000, java.util.concurrent.TimeUnit.MILLISECONDS);
                    if (null != t) {
                        exception = t;
                        break;
                    }
                } catch (TimeoutException timeoutException) {
                    // Ignore timeout exception, this task is not finished yet.
                    done = false;
                } catch (Throwable t) {
                    exception = t;
                    break;
                }
            }

            // If one task fails, cancel all tasks.
            checkException(futures, exception);

            if (done) {
                // All tasks succeed.
                break;
            }

            // Check ddl interrupt flag.
            if (ec.getDdlContext().isInterrupted()) {
                exception = new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_INDEX_CHECKER,
                    "Incremental check is interrupted.");
                break;
            }
        }
        return exception;
    }

    private void addConsumers(long tso, IInnerConnectionManager connectionManager, AtomicBoolean finish,
                              int parallelism, String concatColumnName, ConcurrentLinkedQueue<String> checkInsertValues,
                              List<Future<Throwable>> futures, ServerThreadPool threadPool, AtomicLong count) {
        for (int i = 0; i < parallelism; i++) {
            futures.add(threadPool.submit(null, null, () -> {
                final Map savedMdcContext = MDC.getCopyOfContextMap();
                MDC.put(MDC.MDC_KEY_APP, DEFAULT_DB_NAME);
                String sql = null;
                try {
                    String values = null;
                    // Must get from work queue first, and then check finish flag.
                    while (null != (values = checkInsertValues.poll()) || !finish.get()) {
                        if (null == values) {
                            Thread.sleep(1000);
                            continue;
                        }
                        sql = "select count(0) from " + tableName + " as of tso " + tso + " where "
                            + concatColumnName + " in " + values;
                        try (IInnerConnection conn = connectionManager.getConnection(schemaName);
                            Statement stmt = conn.createStatement()) {
                            connections.add(conn);
                            ResultSet countRs = stmt.executeQuery(sql);
                            if (countRs.next()) {
                                count.addAndGet(countRs.getLong(1));
                            }
                            connections.remove(conn);
                        }
                    }
                } catch (Throwable t) {
                    logger.error("Check cci incremental insert data failed. sql: " + sql, t);
                    return t;
                } finally {
                    MDC.setContextMap(savedMdcContext);
                }
                return null;
            }));
        }
    }

    private void checkException(List<Future<Throwable>> futures, Throwable exception) throws Throwable {
        if (null != exception) {
            futures.forEach(future -> future.cancel(true));
            // Force close all connections.
            connections.forEach(conn -> {
                try {
                    conn.close();
                } catch (Throwable e) {
                    // ignore
                }
            });
            throw exception;
        }
    }

    private ResultSet getIncrementalInsertData(ExecutionContext ec, long tsoV0, long tsoV1,
                                               IInnerConnection conn) throws SQLException {
        // partition_name -> files in this partition
        Map<String, Set<String>> orcFiles;
        Map<String, IDeltaReadOption> deltaFiles;
        // Get v0 orc files.
        long startTime = System.nanoTime();
        final long tableId = ICciChecker.getTableId(schemaName, indexName);
        SQLRecorderLogger.ddlLogger.info(
            "[Incremental checker] Get table id cost: " + (System.nanoTime() - startTime) / 1_000_000 + " ms");

        startTime = System.nanoTime();
        Map<String, Set<String>> orcFilesV0 = new HashMap<>();
        List<FilesRecordSimplifiedWithChecksum> filesRecords = ICciChecker.getFilesRecords(tsoV0, tableId, schemaName);
        SQLRecorderLogger.ddlLogger.info(
            "[Incremental checker] Get all files cost: " + (System.nanoTime() - startTime) / 1_000_000 + " ms");

        for (FilesRecordSimplified filesRecord : filesRecords) {
            String fileName = filesRecord.fileName;
            String partitionName = filesRecord.partitionName.toLowerCase();
            ColumnarFileType columnarFileType =
                ColumnarFileType.of(fileName.substring(fileName.lastIndexOf('.') + 1));
            if (Objects.requireNonNull(columnarFileType) == ColumnarFileType.ORC) {
                orcFilesV0.computeIfAbsent(partitionName, k -> new HashSet<>()).add(fileName);
            }
        }

        // Get v1 orc/csv/del files.
        Map<String, Set<String>> orcFilesV1 = new HashMap<>();
        Set<String> deltaFilesV1 = new HashSet<>();
        filesRecords = ICciChecker.getFilesRecords(tsoV1, tableId, schemaName);
        for (FilesRecordSimplified filesRecord : filesRecords) {
            String fileName = filesRecord.fileName;
            String partitionName = filesRecord.partitionName.toLowerCase();
            ColumnarFileType columnarFileType =
                ColumnarFileType.of(fileName.substring(fileName.lastIndexOf('.') + 1));
            switch (columnarFileType) {
            case ORC:
                orcFilesV1.computeIfAbsent(partitionName, k -> new HashSet<>()).add(fileName);
                break;
            case CSV:
            case DEL:
                deltaFilesV1.add(fileName);
                break;
            default:
                logger.warn("Increment check found unexpected file: " + fileName);
                break;
            }
        }

        // Calculate the difference.
        orcFiles = ExecUtils.diffOrcFiles(orcFilesV0, orcFilesV1);
        startTime = System.nanoTime();
        deltaFiles = ExecUtils.diffDeltaFiles(tsoV0, tsoV1, tableId, deltaFilesV1);
        SQLRecorderLogger.ddlLogger.info(
            "[Incremental checker] Get all delta files info cost: "
                + (System.nanoTime() - startTime) / 1_000_000 + " ms");

        // Make sure all partitions in orcFiles has corresponding delta files.
        for (Map.Entry<String, Set<String>> entry : orcFiles.entrySet()) {
            String partitionName = entry.getKey();
            deltaFiles.putIfAbsent(partitionName,
                new SpecifiedOssSplit.DeltaReadWithPositionOption(tsoV1, tsoV0, tsoV1, tableId));
        }

        if (orcFiles.isEmpty() && deltaFiles.isEmpty()) {
            // v0 and v1 are identical.
            return null;
        }

        ResultSet rs = null;
        try (Statement stmt = conn.createStatement()) {
            conn.addExecutionContextInjectHook(
                (e) -> {
                    ((ExecutionContext) e).setCheckingCci(true);
                    ((ExecutionContext) e).setReadOrcFiles(orcFiles);
                    ((ExecutionContext) e).setReadDeltaFiles(deltaFiles);
                });
            StringBuilder sb = new StringBuilder();
            ICciChecker.setBasicHint(ec, sb);
            sb.append(" SNAPSHOT_TS=")
                .append(tsoV1)
                .append(" ");

            String hint = String.format(COLUMNAR_HINT, sb);
            startTime = System.nanoTime();
            rs = stmt.executeQuery(hint + " select * from " + tableName + " force index(" + indexName + ") ");
            SQLRecorderLogger.ddlLogger.info(
                "[Incremental checker] Get incremental insert data cost: "
                    + (System.nanoTime() - startTime) / 1_000_000 + " ms");
            return rs;
        }
    }

    @Override
    public boolean getCheckReports(Collection<String> reports) {
        if (errors.isEmpty()) {
            String report = String.format("Incremental check passed for schema %s, table %s, index %s , "
                + "increment insert count %s ", schemaName, tableName, indexName, finalCount);
            reports.add(report);
            SQLRecorderLogger.ddlLogger.info(report);
            return true;
        }
        String report = String.format("Incremental check failed for schema %s, table %s, index %s",
            schemaName, tableName, indexName);
        SQLRecorderLogger.ddlLogger.warn(report);
        reports.add(report);
        reports.addAll(errors);
        return false;
    }

}
