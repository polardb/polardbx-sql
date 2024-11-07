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

package com.alibaba.polardbx.executor.columnar.checker;

import com.alibaba.polardbx.common.IInnerConnection;
import com.alibaba.polardbx.common.IInnerConnectionManager;
import com.alibaba.polardbx.common.RevisableOrderInvariantHash;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.oss.ColumnarFileType;
import com.alibaba.polardbx.common.oss.IDeltaReadOption;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.executor.mpp.split.SpecifiedOssSplit;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecordSimplifiedWithChecksum;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

/**
 * @author yaozhili
 */
public class CciFastChecker implements ICciChecker {
    private static final Logger logger = LoggerFactory.getLogger(CciFastChecker.class);

    private final String schemaName;
    protected final String tableName;
    protected final String indexName;
    private long columnarHashCode = -1L;
    private long primaryHashCode = -1L;
    private final List<String> errors = new ArrayList<>();
    private final Lock hashLock = new ReentrantLock();
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

    private static final String CALCULATE_PRIMARY_HASH =
        "select check_sum_v2(*) as checksum from %s force index(primary)";
    private static final String CALCULATE_COLUMNAR_HASH =
        "select check_sum_v2(*) as checksum from %s force index(%s)";

    protected static final String PRIMARY_HINT =
        "/*+TDDL:WORKLOAD_TYPE=AP ENABLE_ORC_RAW_TYPE_BLOCK=true "
            + "SOCKET_TIMEOUT=259200000 MPP_TASK_MAX_RUN_TIME=259200000 %s */";
    protected static final String COLUMNAR_HINT =
        "/*+TDDL:WORKLOAD_TYPE=AP ENABLE_COLUMNAR_OPTIMIZER=true "
            + "OPTIMIZER_TYPE='columnar' ENABLE_HTAP=true ENABLE_BLOCK_CACHE=false ENABLE_ORC_RAW_TYPE_BLOCK=true "
            + "SOCKET_TIMEOUT=259200000 MPP_TASK_MAX_RUN_TIME=259200000 %s */";

    public CciFastChecker(String schemaName, String tableName, String indexName) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.indexName = indexName;
    }

    @Override
    public void check(ExecutionContext baseEc, Runnable recoverChangedConfigs) throws Throwable {
        Pair<Long, Long> tso = getCheckTso();
        logger.warn("Check cci using innodb tso " + tso.getKey() + ", columnar tso " + tso.getValue());

        ITransaction trx = ExecUtils.createColumnarTransaction(schemaName, baseEc, tso.getValue());

        try {
            ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
            ServerThreadPool threadPool = executorContext.getTopologyExecutor().getExecutorService();
            IInnerConnectionManager connectionManager = executorContext.getInnerConnectionManager();

            // Calculate primary table checksum.
            long start = System.nanoTime();
            calculatePrimaryChecksum(baseEc, tso.getKey(), threadPool, connectionManager, recoverChangedConfigs);
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
        } catch (Throwable t) {
            handleError(t);
            throw t;
        } finally {
            trx.close();
        }
    }

    protected Pair<Long, Long> getCheckTso() {
        return ColumnarTransactionUtils.getLatestOrcCheckpointTsoFromGms();
    }

    protected void calculatePrimaryChecksum(ExecutionContext baseEc, long tso,
                                            ServerThreadPool threadPool,
                                            IInnerConnectionManager connectionManager,
                                            Runnable recoverChangedConfigs)
        throws SQLException, ExecutionException, InterruptedException {
        try (IInnerConnection daemonConnection = ICciChecker.startDaemonTransaction(
            connectionManager, schemaName, tableName)) {
            // Calculate primary checksum in this thread.
            try (IInnerConnection conn = connectionManager.getConnection(schemaName);
                Statement stmt = conn.createStatement()) {
                connections.add(conn);
                conn.setTimeZone("+8:00");

                String finalSql = getPrimarySql(baseEc, tso);
                SQLRecorderLogger.ddlLogger.info("primary checksum sql: " + finalSql);
                logger.info("primary checksum sql: " + finalSql);

                executeSqlAndInterruptIfDdlCanceled(threadPool, stmt, finalSql, baseEc, true);
            } catch (Throwable t) {
                handleError(t);
                throw t;
            } finally {
                connections.clear();
                if (null != recoverChangedConfigs) {
                    recoverChangedConfigs.run();
                }
            }
            try {
                daemonConnection.close();
            } catch (Throwable t) {
                // ignore.
            }
        }
    }

    private void executeSqlAndInterruptIfDdlCanceled(ServerThreadPool threadPool, Statement stmt, String finalSql,
                                                     ExecutionContext baseEc, boolean isPrimary)
        throws InterruptedException, ExecutionException {
        Future<String> future = threadPool.submit(null, null, () -> {
            try {
                ResultSet rs = stmt.executeQuery(finalSql);
                if (rs.next()) {
                    if (isPrimary) {
                        primaryHashCode = rs.getLong("checksum");
                    } else {
                        columnarHashCode = rs.getLong("checksum");
                    }
                }
                return null;
            } catch (SQLException e) {
                handleError(e);
                return e.getMessage();
            }
        });

        String error = null;
        while (true) {
            try {
                error = future.get(1, TimeUnit.SECONDS);
                break;
            } catch (TimeoutException e) {
                // ignore.
            }

            if (baseEc.getDdlContext().isInterrupted()) {
                future.cancel(true);
                if (isPrimary) {
                    primaryHashCode = -1;
                } else {
                    columnarHashCode = -1;
                }
                error = String.format("Interrupted when checking columnar index %s.%s", tableName, indexName);
                break;
            }
        }
        if (null != error) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_INDEX_CHECKER,
                "Fast CCI checker error: " + error);
        }
    }

    protected void calculateColumnarChecksum(ExecutionContext baseEc,
                                             long tso,
                                             ServerThreadPool threadPool,
                                             IInnerConnectionManager connectionManager) throws SQLException {
        // 1. Get all orc/csv files of this CCI.
        long startTime = System.nanoTime();
        final long tableId = ICciChecker.getTableId(schemaName, indexName);
        SQLRecorderLogger.ddlLogger.info(
            "[Fast checker] Get table id cost: " + (System.nanoTime() - startTime) / 1_000_000 + " ms");

        startTime = System.nanoTime();
        List<FilesRecordSimplifiedWithChecksum> filesRecords = ICciChecker.getFilesRecords(tso, tableId, schemaName);
        SQLRecorderLogger.ddlLogger.info(
            "[Fast checker] Get all files cost: " + (System.nanoTime() - startTime) / 1_000_000 + " ms");

        Map<String, Set<String>> orcFiles = new HashMap<>();
        Set<String> deltaFiles = new HashSet<>();
        // 2. Filter out files needed to be process, and by the way calculate cached checksum.
        final RevisableOrderInvariantHash hasher = new RevisableOrderInvariantHash();
        final Map<String, Set<String>> toBeProcessedOrcFiles = new HashMap<>();
        for (FilesRecordSimplifiedWithChecksum filesRecord : filesRecords) {
            String fileName = filesRecord.fileName;
            String partitionName = filesRecord.partitionName.toLowerCase();
            ColumnarFileType columnarFileType =
                ColumnarFileType.of(fileName.substring(fileName.lastIndexOf('.') + 1));
            switch (columnarFileType) {
            case ORC:
                orcFiles.computeIfAbsent(partitionName, k -> new HashSet<>()).add(fileName);
                hasher.add(filesRecord.checksum).remove(0);
                if (0 != filesRecord.deletedChecksum) {
                    toBeProcessedOrcFiles.computeIfAbsent(partitionName, k -> new HashSet<>()).add(fileName);
                }
                break;
            case CSV:
            case DEL:
                deltaFiles.add(fileName);
                break;
            default:
                logger.warn("Increment check found unexpected file: " + fileName);
                break;
            }
        }
        SQLRecorderLogger.ddlLogger.info("all orc files checksum: " + hasher.getResult());
        logger.info("all orc files checksum: " + hasher.getResult());

        // csv/del file name -> pair(partition name, end pos)
        startTime = System.nanoTime();
        Map<String, Pair<String, Long>> tuples = new HashMap<>();
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarAppendedFilesAccessor accessor = new ColumnarAppendedFilesAccessor();
            accessor.setConnection(connection);
            accessor.queryByFileNamesAndTso(deltaFiles, tso).forEach(record -> {
                String fileName = record.getFileName();
                long start = record.getAppendOffset();
                long end = start + record.getAppendLength();
                String partName = record.getPartName();
                tuples.put(fileName, new Pair<>(partName, end));
            });
        } catch (Throwable t) {
            logger.error("calculate columnar checksum failed.", t);
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_INDEX_CHECKER, t, "Failed to diff csv files");
        }
        SQLRecorderLogger.ddlLogger.info(
            "[Fast checker] Get all delta files info cost: " + (System.nanoTime() - startTime) / 1_000_000 + " ms");

        Map<String, IDeltaReadOption> deltas = new HashMap<>();
        AtomicBoolean hasCsvFiles = new AtomicBoolean(false);
        for (Map.Entry<String, Pair<String, Long>> deltaFileEntry : tuples.entrySet()) {
            String fileName = deltaFileEntry.getKey();
            String partName = deltaFileEntry.getValue().getKey();
            long endPos = deltaFileEntry.getValue().getValue();
            deltas.compute(partName, (k, v) -> {
                if (v == null) {
                    v = new SpecifiedOssSplit.DeltaReadWithPositionOption(tso, -1, -1, tableId);
                }
                ColumnarFileType columnarFileType =
                    ColumnarFileType.of(fileName.substring(fileName.lastIndexOf('.') + 1));
                SpecifiedOssSplit.DeltaReadWithPositionOption delta = (SpecifiedOssSplit.DeltaReadWithPositionOption) v;
                switch (columnarFileType) {
                case CSV:
                    hasCsvFiles.set(true);
                    if (delta.getCsvFiles() == null) {
                        delta.setCsvFiles(new ArrayList<>());
                        delta.setCsvStartPos(new ArrayList<>());
                        delta.setCsvEndPos(new ArrayList<>());
                    }
                    delta.getCsvFiles().add(fileName);
                    delta.getCsvStartPos().add(0L);
                    delta.getCsvEndPos().add(endPos);
                    break;
                case DEL:
                    if (delta.getDelFiles() == null) {
                        delta.setDelFiles(new ArrayList<>());
                        delta.setDelBeginPos(new ArrayList<>());
                        delta.setDelEndPos(new ArrayList<>());
                    }
                    delta.getDelFiles().add(fileName);
                    delta.getDelBeginPos().add(0L);
                    delta.getDelEndPos().add(endPos);
                    break;
                default:
                    logger.warn("Cci fast check found unexpected file: " + fileName);
                    break;
                }
                return v;
            });
        }

        CompletableFuture<String> deletedChecksumFuture = null;
        // 3. RTT 2: Calculate deleted checksum.
        if (!toBeProcessedOrcFiles.isEmpty()) {
            deletedChecksumFuture = CompletableFuture.supplyAsync(
                () -> calDeletedChecksum(baseEc, connectionManager, hasher,
                    toBeProcessedOrcFiles, deltas), threadPool);
        }

        // 4. RTT 2: Calculate csv part in this thread.
        CompletableFuture<String> csvFuture = null;
        if (hasCsvFiles.get()) {
            csvFuture = CompletableFuture.supplyAsync(
                () -> calCsvChecksum(baseEc, connectionManager, hasher, deltas), threadPool);
        }

        // Combine these two tasks.
        CompletableFuture<String> completableFuture = null;
        if (null == deletedChecksumFuture) {
            if (null != csvFuture) {
                completableFuture = csvFuture;
            }
        } else {
            if (null == csvFuture) {
                completableFuture = deletedChecksumFuture;
            } else {
                completableFuture = deletedChecksumFuture.thenCombine(csvFuture, (s1, s2) -> {
                    if (null == s1 && null == s2) {
                        return null;
                    }
                    return s1 + "\n" + s2;
                });
            }
        }

        // If ddl is paused, interrupt tasks.
        String error = null;
        if (null != completableFuture) {
            while (true) {
                try {
                    error = completableFuture.get(1, TimeUnit.SECONDS);
                    break;
                } catch (TimeoutException e) {
                    // ignore.
                } catch (Throwable t) {
                    error = t.getMessage();
                    break;
                }

                if (baseEc.getDdlContext().isInterrupted()) {
                    completableFuture.cancel(true);
                    error = String.format("Interrupted when checking columnar index %s.%s", tableName, indexName);
                    break;
                }
            }
        }

        if (null != error) {
            for (IInnerConnection connection : connections) {
                connection.close();
            }
            columnarHashCode = -1;
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_INDEX_CHECKER,
                "Fast CCI checker error: " + error);
        } else {
            columnarHashCode = hasher.getResult();
        }

        if (-1 != columnarHashCode && -1 != primaryHashCode && columnarHashCode != primaryHashCode) {
            // Use naive method to check again.
            calColumnarChecksumInNaiveMethod(orcFiles, deltas, baseEc, tso, threadPool, connectionManager);
        }
    }

    @Nullable
    private String calCsvChecksum(ExecutionContext baseEc, IInnerConnectionManager connectionManager,
                                  RevisableOrderInvariantHash hasher, Map<String, IDeltaReadOption> deltas) {
        final Map savedMdcContext = MDC.getCopyOfContextMap();
        MDC.put(MDC.MDC_KEY_APP, DEFAULT_DB_NAME);
        try (IInnerConnection conn = connectionManager.getConnection(schemaName);
            Statement stmt = conn.createStatement()) {
            connections.add(conn);
            conn.addExecutionContextInjectHook(
                e -> {
                    ((ExecutionContext) e).setCheckingCci(true);
                    ((ExecutionContext) e).setReadDeltaFiles(deltas);
                }
            );
            String sql = getCsvSql(baseEc);

            SQLRecorderLogger.ddlLogger.info("columnar csv checksum sql: " + sql);
            logger.info("columnar csv checksum sql: " + sql);

            long startTime = System.nanoTime();
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                long csvChecksum = rs.getLong("checksum");
                SQLRecorderLogger.ddlLogger.info("columnar csv checksum: " + csvChecksum);
                logger.info("columnar csv checksum: " + csvChecksum);
                hashLock.lock();
                try {
                    hasher.add(csvChecksum).remove(0);
                } finally {
                    hashLock.unlock();
                }
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_INDEX_CHECKER, "not found any csv checksum.");
            }
            SQLRecorderLogger.ddlLogger.info(
                "Calculate csv checksum cost: " + (System.nanoTime() - startTime) / 1_000_000 + " ms");

            conn.clearExecutionContextInjectHooks();
            connections.remove(conn);
            return null;
        } catch (Throwable t) {
            handleError(t);
            return t.getMessage();
        } finally {
            MDC.setContextMap(savedMdcContext);
        }
    }

    @Nullable
    private String calDeletedChecksum(ExecutionContext baseEc,
                                      IInnerConnectionManager connectionManager,
                                      RevisableOrderInvariantHash hasher,
                                      Map<String, Set<String>> toBeProcessedOrcFiles,
                                      Map<String, IDeltaReadOption> deltas) {
        final Map savedMdcContext = MDC.getCopyOfContextMap();
        MDC.put(MDC.MDC_KEY_APP, DEFAULT_DB_NAME);
        try (IInnerConnection conn = connectionManager.getConnection(schemaName);
            Statement stmt = conn.createStatement()) {
            connections.add(conn);
            conn.addExecutionContextInjectHook(
                (ec) -> {
                    ((ExecutionContext) ec).setCheckingCci(true);
                    ((ExecutionContext) ec).setReadOrcFiles(toBeProcessedOrcFiles);
                    ((ExecutionContext) ec).setReadDeltaFiles(deltas);
                });
            String sql = getDeletedSql(baseEc);

            SQLRecorderLogger.ddlLogger.info("columnar deleted checksum sql: " + sql);
            logger.info("columnar deleted checksum sql: " + sql);

            long startTime = System.nanoTime();
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                long deletedChecksum = rs.getLong("checksum");
                SQLRecorderLogger.ddlLogger.info("columnar deleted checksum: " + deletedChecksum);
                logger.info("columnar deleted checksum: " + deletedChecksum);
                hashLock.lock();
                try {
                    hasher.remove(deletedChecksum).add(0);
                } finally {
                    hashLock.unlock();
                }
            } else {
                return "Not found deleted checksum in result set.";
            }
            SQLRecorderLogger.ddlLogger.info(
                "Calculate deleted checksum cost: " + (System.nanoTime() - startTime) / 1_000_000 + " ms");

            conn.clearExecutionContextInjectHooks();
            connections.remove(conn);
        } catch (Throwable t) {
            handleError(t);
            return "Error occurs, caused by " + t.getMessage();
        } finally {
            MDC.setContextMap(savedMdcContext);
        }
        return null;
    }

    private void calColumnarChecksumInNaiveMethod(Map<String, Set<String>> orcFiles,
                                                  Map<String, IDeltaReadOption> deltas,
                                                  ExecutionContext baseEc, long tso, ServerThreadPool threadPool,
                                                  IInnerConnectionManager connectionManager) {
        try (IInnerConnection conn = connectionManager.getConnection(schemaName);
            Statement stmt = conn.createStatement()) {
            conn.addExecutionContextInjectHook(
                (ec) -> {
                    ((ExecutionContext) ec).setCheckingCci(true);
                    ((ExecutionContext) ec).setReadOrcFiles(orcFiles);
                    ((ExecutionContext) ec).setReadDeltaFiles(deltas);
                });
            connections.add(conn);

            String finalSql = getColumnarNaiveSql(baseEc);
            SQLRecorderLogger.ddlLogger.info("columnar naive checksum sql: " + finalSql);
            logger.info("columnar naive checksum sql: " + finalSql);

            executeSqlAndInterruptIfDdlCanceled(threadPool, stmt, finalSql, baseEc, false);
            conn.clearExecutionContextInjectHooks();
            connections.remove(conn);
        } catch (Throwable t) {
            handleError(t);
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_INDEX_CHECKER,
                "Fast CCI checker error: " + t.getMessage());
        }
    }

    protected void handleError(Throwable t) {
        SQLRecorderLogger.ddlLogger.error(String.format(
            "[Fast CCI Checker] Error occurs when checking columnar index %s.%s", tableName, indexName), t);
        errors.add(t.getMessage());
    }

    @Override
    public boolean getCheckReports(Collection<String> reports) {
        boolean success = true;
        if (!errors.isEmpty()) {
            reports.addAll(errors);
        }
        if (-1 == primaryHashCode || primaryHashCode != columnarHashCode) {
            // Check fail.
            reports.add("Inconsistency detected: primary hash: " + primaryHashCode
                + ", columnar hash: " + columnarHashCode);
            success = false;
        }
        return success;
    }

    protected String getPrimarySql(ExecutionContext baseEc, long tso) {
        StringBuilder sb = new StringBuilder();
        ICciChecker.setBasicHint(baseEc, sb);
        sb.append(" SNAPSHOT_TS=")
            .append(tso);
        sb.append(" TRANSACTION_POLICY=TSO");
        String hint = String.format(PRIMARY_HINT, sb);
        return hint + String.format(CALCULATE_PRIMARY_HASH, tableName);
    }

    protected String getCsvSql(ExecutionContext baseEc) {
        StringBuilder sb = new StringBuilder(" READ_CSV_ONLY=true");
        ICciChecker.setBasicHint(baseEc, sb);

        String hint = String.format(COLUMNAR_HINT, sb);
        return hint + String.format(CALCULATE_COLUMNAR_HASH, tableName, indexName);
    }

    protected String getDeletedSql(ExecutionContext baseEc) {
        StringBuilder sb = new StringBuilder(" READ_ORC_ONLY=true ENABLE_OSS_DELETED_SCAN=true ");
        ICciChecker.setBasicHint(baseEc, sb);

        String hint = String.format(COLUMNAR_HINT, sb);
        return hint + String.format(CALCULATE_COLUMNAR_HASH, tableName, indexName);
    }

    protected String getColumnarNaiveSql(ExecutionContext baseEc) {
        StringBuilder sb = new StringBuilder(" READ_SPECIFIED_COLUMNAR_FILES=true ");
        ICciChecker.setBasicHint(baseEc, sb);
        String hint = String.format(COLUMNAR_HINT, sb);
        return hint + String.format(CALCULATE_COLUMNAR_HASH, tableName, indexName);
    }

    public long getColumnarHashCode() {
        return columnarHashCode;
    }
}
