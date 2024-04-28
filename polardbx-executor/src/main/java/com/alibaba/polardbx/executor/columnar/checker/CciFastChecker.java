package com.alibaba.polardbx.executor.columnar.checker;

import com.alibaba.polardbx.common.IInnerConnection;
import com.alibaba.polardbx.common.IInnerConnectionManager;
import com.alibaba.polardbx.common.RevisableOrderInvariantHash;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartSpecBase;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

/**
 * @author yaozhili
 */
public class CciFastChecker implements ICciChecker {
    private static final Logger logger = LoggerFactory.getLogger(CciFastChecker.class);

    private final String schemaName;
    private final String tableName;
    private final String indexName;
    private long columnarHashCode = -1;
    private long primaryHashCode = -1;
    /**
     * Record connection id in use.
     * If the checking thread is interrupted, kill these connections.
     */
    private List<Long> connIds = new ArrayList<>();

    private static final String CALCULATE_PRIMARY_HASH =
        "select check_sum_v2(*) as checksum from %s force index(primary)";
    private static final String CALCULATE_COLUMNAR_HASH =
        "select check_sum_v2(*) as checksum from %s force index(%s)";

    private static final String PRIMARY_HINT =
        "/*+TDDL:WORKLOAD_TYPE=AP ENABLE_MPP=true ENABLE_MASTER_MPP=true ENABLE_ORC_RAW_TYPE_BLOCK=true "
            + "SOCKET_TIMEOUT=259200000 MPP_TASK_MAX_RUN_TIME=259200000 %s */";
    private static final String COLUMNAR_HINT =
        "/*+TDDL:WORKLOAD_TYPE=AP ENABLE_MPP=true ENABLE_MASTER_MPP=true ENABLE_COLUMNAR_OPTIMIZER=true "
            + "OPTIMIZER_TYPE='columnar' ENABLE_HTAP=true ENABLE_ORC_RAW_TYPE_BLOCK=true "
            + "SOCKET_TIMEOUT=259200000 MPP_TASK_MAX_RUN_TIME=259200000 %s */";

    public CciFastChecker(String schemaName, String tableName, String indexName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.indexName = indexName;
    }

    @Override
    public void check(ExecutionContext baseEc, Runnable recoverChangedConfigs) throws Throwable {
        Pair<Long, Long> tso =
            ColumnarTransactionUtils.getLatestOrcCheckpointTsoFromGms();
        logger.warn("Check cci using innodb tso " + tso.getKey() + ", columnar tso " + tso.getValue());

        ITransaction trx = ExecUtils.createColumnarTransaction(schemaName, baseEc, tso.getValue());

        try {
            ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
            ServerThreadPool threadPool = executorContext.getTopologyExecutor().getExecutorService();
            IInnerConnectionManager connectionManager = executorContext.getInnerConnectionManager();

            // Calculate primary table checksum.
            long start = System.nanoTime();
            calculatePrimaryChecksum(baseEc, tso.getKey(), connectionManager, recoverChangedConfigs);
            SQLRecorderLogger.ddlLogger.info("[Fast checker] Primary checksum calculated, costing "
                + ((System.nanoTime() - start) / 1_000_000) + " ms");

            // Calculate columnar table checksum.
            start = System.nanoTime();
            calculateColumnarChecksum(baseEc, tso.getValue(), threadPool, connectionManager);
            SQLRecorderLogger.ddlLogger.info("[Fast checker] Columnar checksum calculated, costing "
                + ((System.nanoTime() - start) / 1_000_000) + " ms");

            SQLRecorderLogger.ddlLogger.info("primary checksum: " + primaryHashCode);
            logger.info("primary checksum: " + primaryHashCode);
            SQLRecorderLogger.ddlLogger.info("columnar checksum: " + columnarHashCode);
            logger.info("columnar checksum: " + columnarHashCode);
        } finally {
            trx.close();
        }
    }

    private void calculatePrimaryChecksum(ExecutionContext baseEc, long tso,
                                          IInnerConnectionManager connectionManager,
                                          Runnable recoverChangedConfigs) {
        // Calculate primary checksum in this thread.
        try (Connection conn = connectionManager.getConnection(schemaName);
            Statement stmt = conn.createStatement()) {

            if (conn instanceof IInnerConnection) {
                ((IInnerConnection) conn).setTimeZone("+8:00");
            }

            // Build hint.
            StringBuilder sb = new StringBuilder();
            long parallelism;
            if ((parallelism = baseEc.getParamManager().getInt(ConnectionParams.MPP_PARALLELISM)) > 0) {
                sb.append(" MPP_PARALLELISM=")
                    .append(parallelism);
            }
            if ((parallelism = baseEc.getParamManager().getInt(ConnectionParams.PARALLELISM)) > 0) {
                sb.append(" PARALLELISM=")
                    .append(parallelism);
            }
            sb.append(" SNAPSHOT_TS=")
                .append(tso);
            sb.append(" TRANSACTION_POLICY=TSO");
            String hint = String.format(PRIMARY_HINT, sb);

            String sql = String.format(CALCULATE_PRIMARY_HASH, tableName);

            // Assert using logical view as table scan.
            sql = hint + sql;
            SQLRecorderLogger.ddlLogger.info("primary checksum sql: " + sql);
            logger.info("primary checksum sql: " + sql);

            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                primaryHashCode = rs.getLong("checksum");
            }
        } catch (Throwable t) {
            SQLRecorderLogger.ddlLogger.error(
                String.format("Error occurs when checking columnar index %s.%s", tableName, indexName), t);
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_INDEX_CHECKER,
                "Fast CCI checker error: " + t.getMessage());
        } finally {
            if (null != recoverChangedConfigs) {
                recoverChangedConfigs.run();
            }
        }
    }

    private void calculateColumnarChecksum(ExecutionContext baseEc,
                                           long tso,
                                           ServerThreadPool threadPool,
                                           IInnerConnectionManager connectionManager) {
        // 1. Get all orc files of this CCI.
        ColumnarManager columnarManager = ColumnarManager.getInstance();
        TableMeta tableMeta = baseEc.getSchemaManager(schemaName).getTable(indexName);
        List<String> orcFiles = new ArrayList<>();
        List<String> csvFiles = new ArrayList<>();
        tableMeta.getPartitionInfo()
            .getPartitionBy()
            .getPartitions()
            .stream()
            // Get each partition name.
            .map(PartSpecBase::getName)
            // Get files from each partition.
            .forEach(partitionName -> {
                Pair<List<String>, List<String>> orcAndCsv =
                    columnarManager.findFileNames(tso, schemaName, indexName, partitionName);
                orcFiles.addAll(orcAndCsv.getKey());
                csvFiles.addAll(orcAndCsv.getValue());
            });
        // RTT 1: get file records.
        List<FilesRecord> filesRecords = ExecUtils.getFilesMetaByNames(orcFiles);
        if (filesRecords.size() != orcFiles.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_INDEX_CHECKER, "The number of orc files not match");
        }

        // 2. Filter out files needed to be process, and by the way calculate cached checksum.
        final RevisableOrderInvariantHash hasher = new RevisableOrderInvariantHash();
        final List<String> toBeProcessedFiles = new ArrayList<>();
        for (FilesRecord filesRecord : filesRecords) {
            hasher.add(filesRecord.checksum).remove(0);
            if (0 != filesRecord.deletedChecksum) {
                toBeProcessedFiles.add(filesRecord.fileName);
            }
        }

        SQLRecorderLogger.ddlLogger.info("all orc files checksum: " + hasher.getResult());
        logger.info("all orc files checksum: " + hasher.getResult());

        Future<String> future = null;
        // 3. RTT 2: Calculate deleted checksum.
        if (!toBeProcessedFiles.isEmpty()) {
            future = threadPool.submit(null, null, () -> {
                try (Connection conn = connectionManager.getConnection(schemaName);
                    Statement stmt = conn.createStatement()) {
                    if (conn instanceof IInnerConnection) {
                        ((IInnerConnection) conn).addExecutionContextInjectHook(
                            (ec) -> {
                                ((ExecutionContext) ec).setCheckingCci(true);
                                ((ExecutionContext) ec).setReadOrcFiles(toBeProcessedFiles);
                            });
                    }
                    StringBuilder sb =
                        new StringBuilder(
                            " READ_ORC_ONLY=true ENABLE_ORC_DELETED_SCAN=true ");
                    long parallelism;
                    if ((parallelism = baseEc.getParamManager().getInt(ConnectionParams.MPP_PARALLELISM)) > 0) {
                        sb.append(" MPP_PARALLELISM=")
                            .append(parallelism);
                    }
                    if ((parallelism = baseEc.getParamManager().getInt(ConnectionParams.PARALLELISM)) > 0) {
                        sb.append(" PARALLELISM=")
                            .append(parallelism);
                    }

                    String hint = String.format(COLUMNAR_HINT, sb);
                    String sql = hint + String.format(CALCULATE_COLUMNAR_HASH, tableName, indexName);

                    SQLRecorderLogger.ddlLogger.info("columnar deleted checksum sql: " + sql);
                    logger.info("columnar deleted checksum sql: " + sql);

                    ResultSet rs = stmt.executeQuery(sql);
                    if (rs.next()) {
                        long deletedChecksum = rs.getLong("checksum");
                        SQLRecorderLogger.ddlLogger.info("columnar deleted checksum: " + deletedChecksum);
                        logger.info("columnar deleted checksum: " + deletedChecksum);
                        hasher.remove(deletedChecksum).add(0);
                    } else {
                        return "Not found deleted checksum in result set.";
                    }

                    if (conn instanceof IInnerConnection) {
                        ((IInnerConnection) conn).clearExecutionContextInjectHooks();
                    }
                } catch (Throwable t) {
                    SQLRecorderLogger.ddlLogger.error(t);
                    return "Error occurs, caused by " + t.getMessage();
                }
                return null;
            });
        }

        // 4. RTT 2: Calculate csv part in this thread.
        if (!csvFiles.isEmpty()) {
            try (Connection conn = connectionManager.getConnection(schemaName);
                Statement stmt = conn.createStatement()) {
                if (conn instanceof IInnerConnection) {
                    ((IInnerConnection) conn).addExecutionContextInjectHook(
                        (ec) -> ((ExecutionContext) ec).setCheckingCci(true));
                }
                StringBuilder sb = new StringBuilder(" READ_CSV_ONLY=true");
                long parallelism;
                if ((parallelism = baseEc.getParamManager().getInt(ConnectionParams.MPP_PARALLELISM)) > 0) {
                    sb.append(" MPP_PARALLELISM=")
                        .append(parallelism);
                }
                if ((parallelism = baseEc.getParamManager().getInt(ConnectionParams.PARALLELISM)) > 0) {
                    sb.append(" PARALLELISM=")
                        .append(parallelism);
                }
                sb.append(" SNAPSHOT_TS=")
                    .append(tso)
                    .append(" ");

                String hint = String.format(COLUMNAR_HINT, sb);
                String sql = hint + String.format(CALCULATE_COLUMNAR_HASH, tableName, indexName);

                SQLRecorderLogger.ddlLogger.info("columnar csv checksum sql: " + sql);
                logger.info("columnar csv checksum sql: " + sql);

                ResultSet rs = stmt.executeQuery(sql);
                if (rs.next()) {
                    long csvChecksum = rs.getLong("checksum");
                    SQLRecorderLogger.ddlLogger.info("columnar csv checksum: " + csvChecksum);
                    logger.info("columnar csv checksum: " + csvChecksum);
                    hasher.add(csvChecksum).remove(0);
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_INDEX_CHECKER, "not found any csv checksum.");
                }

                if (conn instanceof IInnerConnection) {
                    ((IInnerConnection) conn).clearExecutionContextInjectHooks();
                }
            } catch (Throwable t) {
                if (null != future) {
                    future.cancel(true);
                }
                SQLRecorderLogger.ddlLogger.error(t);
                throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_INDEX_CHECKER, t.getMessage());
            }
        }

        try {
            String error = null == future ? null : future.get();
            if (null != error) {
                throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_INDEX_CHECKER, error);
            }
        } catch (Throwable t) {
            SQLRecorderLogger.ddlLogger.error(t);
            future.cancel(true);
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_INDEX_CHECKER, t.getMessage());
        }

        columnarHashCode = hasher.getResult();
    }

    @Override
    public boolean getCheckReports(Collection<String> reports) {
        boolean success = true;
        if (-1 == primaryHashCode || primaryHashCode != columnarHashCode) {
            // Check fail.
            reports.add("Inconsistency detected: primary hash: " + primaryHashCode
                + ", columnar hash: " + columnarHashCode);
            success = false;
        }
        return success;
    }
}
