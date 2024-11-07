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

package com.alibaba.polardbx.executor.scheduler.executor.trx;

import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.trx.TrxLogTableConstants;
import com.alibaba.polardbx.common.utils.AsyncUtils;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.util.lang.Consumer;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.spi.ITopologyExecutor;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.metadb.trx.TrxLogStatusAccessor;
import com.alibaba.polardbx.gms.metadb.trx.TrxLogStatusRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.CREATE_GLOBAL_TX_TABLE_V2;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.CREATE_GLOBAL_TX_TABLE_V2_TMP;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.DROP_GLOBAL_TX_TABLE_V2_ARCHIVE;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.DROP_GLOBAL_TX_TABLE_V2_TMP;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.FORCE_RENAME_GLOBAL_TX_TABLE_V2;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.QUERY_TABLE_ROWS_TX_TABLE_V2;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.SELECT_MAX_TX_ID_IN_ARCHIVE;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.SELECT_TABLE_ROWS_V2;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.SET_DISTRIBUTED_TRX_ID;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.SHOW_ALL_GLOBAL_TX_TABLE_V2;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.SWITCH_GLOBAL_TX_TABLE_V2;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.TRX_LOG_SOCKET_TIMEOUT;
import static com.alibaba.polardbx.common.utils.LockUtil.wrapWithLockWaitTimeout;
import static com.alibaba.polardbx.executor.utils.ExecUtils.scanRecoveredTrans;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_TRX_LOG_TB_FAILED_BEFORE_CREATE_TMP;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_TRX_LOG_TB_FAILED_BEFORE_DROP_TABLE;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_TRX_LOG_TB_FAILED_BEFORE_SWITCH_TABLE;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_TRX_LOG_TB_FAILED_DURING_CREATE_TMP;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_TRX_LOG_TB_FAILED_DURING_DROP_TABLE;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_TRX_LOG_TB_FAILED_DURING_SWITCH_TABLE;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.CDC_DB_NAME;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

/**
 * @author yaozhili
 */
public class CleanLogTableTask {
    private static final Logger logger = LoggerFactory.getLogger(CleanLogTableTask.class);

    public static long run(boolean force, StringBuffer remark) throws SQLException {
        Set<String> dnIds = new HashSet<>();
        Set<String> addresses = new HashSet<>();
        for (StorageInstHaContext ctx : StorageHaManager.getInstance().getMasterStorageList()) {
            // Filter same host:port.
            if (addresses.add(ctx.getCurrAvailableNodeAddr())) {
                dnIds.add(ctx.getStorageInstId());
            }
        }

        TrxLogStatusAccessor accessor = new TrxLogStatusAccessor();
        AtomicLong purged = new AtomicLong(0);
        boolean updateRemark = true;
        try (Connection connection = MetaDbUtil.getConnection()) {
            accessor.setConnection(connection);
            while (true) {
                try {
                    accessor.begin();
                    List<TrxLogStatusRecord> records = accessor.getRecord(true);
                    if (records.isEmpty()) {
                        remark.append("Not found meta, try to init. ");
                        // Init.
                        accessor.insertInitialValue();
                        accessor.commit();

                        EventLogger.log(EventType.TRX_INFO, "Init trx log v2 meta succeed.");

                        // Re-open trx.
                        accessor.begin();
                        records = accessor.getRecord(true);
                        remark.append("Init meta success. ");
                    }

                    if (records.size() != 1) {
                        String err = "Number of trx-log-status record != 1, but is " + records.size();
                        EventLogger.log(EventType.TRX_LOG_ERR, err);
                        throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG, err);
                    }

                    int status = records.get(0).status;

                    remark.append("Current status: ").append(status).append(". ");

                    long before = System.currentTimeMillis();
                    switch (status) {
                    case 0:
                        long lastUpdate = records.get(0).gmtModified.getTime();
                        long currentTime = records.get(0).now.getTime();
                        long minuteToMillis = DynamicConfig.getInstance().getTrxLogCleanInterval() * 60 * 1000;
                        if (!force && lastUpdate + minuteToMillis > currentTime) {
                            updateRemark = false;
                            remark.append("Not in expected time, wait for next round. ");
                            return -1;
                        }

                        // Check if we should clean trx log.
                        if (!force && !shouldCleanTrxLog(dnIds, lastUpdate, currentTime)) {
                            updateRemark = false;
                            remark.append("Too few records, no need to clean. ");
                            return -1;
                        }

                        remark.append("Create tmp table. ");

                        if (FailPoint.isKeyEnable(FP_TRX_LOG_TB_FAILED_BEFORE_CREATE_TMP)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_FAIL,
                                "Fail point FP_TRX_LOG_TB_FAILED_BEFORE_CREATE_TMP");
                        }

                        createTmpTable(dnIds);

                        if (FailPoint.isKeyEnable(FP_TRX_LOG_TB_FAILED_DURING_CREATE_TMP)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_FAIL,
                                "Fail point FP_TRX_LOG_TB_FAILED_DURING_CREATE_TMP");
                        }

                        break;
                    case 1:
                        remark.append("Switch tmp table. ");

                        if (FailPoint.isKeyEnable(FP_TRX_LOG_TB_FAILED_BEFORE_SWITCH_TABLE)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_FAIL,
                                "Fail point FP_TRX_LOG_TB_FAILED_BEFORE_SWITCH_TABLE");
                        }

                        switchTables(dnIds, remark);

                        if (FailPoint.isKeyEnable(FP_TRX_LOG_TB_FAILED_DURING_SWITCH_TABLE)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_FAIL,
                                "Fail point FP_TRX_LOG_TB_FAILED_DURING_SWITCH_TABLE");
                        }

                        break;
                    case 2:
                        remark.append("Drop archive table. ");

                        if (FailPoint.isKeyEnable(FP_TRX_LOG_TB_FAILED_BEFORE_DROP_TABLE)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_FAIL,
                                "Fail point FP_TRX_LOG_TB_FAILED_BEFORE_DROP_TABLE");
                        }

                        dropTable(dnIds, remark, purged);

                        if (FailPoint.isKeyEnable(FP_TRX_LOG_TB_FAILED_DURING_DROP_TABLE)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_FAIL,
                                "Fail point FP_TRX_LOG_TB_FAILED_DURING_DROP_TABLE");
                        }

                        break;
                    default:
                        // Found unexpected status code. Reset it to 0.
                        accessor.updateStatus(0);
                        accessor.commit();
                        throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG, "Unknown trx log status " + status);
                    }

                    remark.append("Done, cost ")
                        .append(System.currentTimeMillis() - before)
                        .append("ms. ");

                    accessor.updateStatus((status + 1) % 3);
                    accessor.commit();

                    if (status == 2) {
                        remark.append("Finish! ");
                        // Done.
                        break;
                    }
                } finally {
                    accessor.rollback();
                }
            }

        } catch (Throwable t) {
            remark.append("Error occurs, ")
                .append(t.getMessage())
                .append(". ");
            logger.error("Clean trx log table v2 failed.", t);
            throw t;
        } finally {
            if (updateRemark) {
                try (Connection connection = MetaDbUtil.getConnection()) {
                    accessor.setConnection(connection);
                    accessor.updateRemark(remark.toString());
                }
            }
        }
        return purged.get();
    }

    /**
     * Clean trx log table if :
     * 1. Some trx log table contains more than 100k rows.
     * 2. It has been more than 1 day since last clean, and we are in maintenance window.
     */
    protected static boolean shouldCleanTrxLog(Set<String> dnIds, long lastUpdate, long currentTime) {
        if ((lastUpdate + 24 * 60 * 60 * 1000 < currentTime) && InstConfUtil.isInMaintenanceTimeWindow()) {
            return true;
        }
        ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();
        AtomicBoolean shouldClean = new AtomicBoolean(false);
        Collection<Future> futures = forEachDn(dnIds, (dnId) -> {
            try (Connection conn = DbTopologyManager.getConnectionForStorage(dnId);
                Statement stmt = conn.createStatement()) {
                conn.setNetworkTimeout(TGroupDirectConnection.socketTimeoutExecutor, TRX_LOG_SOCKET_TIMEOUT);
                wrapWithLockWaitTimeout(conn, 3, () -> {
                    try {
                        ResultSet rs = stmt.executeQuery(QUERY_TABLE_ROWS_TX_TABLE_V2);
                        while (rs.next()) {
                            int rows = rs.getInt(1);
                            if (rows > 100_000) {
                                shouldClean.set(true);
                            }
                        }
                    } catch (Exception e) {
                        EventLogger.log(EventType.TRX_LOG_ERR,
                            "Error during creating tmp table, caused by " + e.getMessage());
                        exceptions.offer(e);
                    }
                });
            } catch (Exception e) {
                exceptions.offer(e);
            }
        });

        AsyncUtils.waitAll(futures);

        exceptions.forEach(logger::error);

        return shouldClean.get();
    }

    private static void createTmpTable(Set<String> dnIds) {
        ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();
        AtomicBoolean lock = new AtomicBoolean(false);
        Collection<Future> futures = forEachDn(dnIds, (dnId) -> {
            try (Connection conn = DbTopologyManager.getConnectionForStorage(dnId);
                Statement stmt = conn.createStatement()) {
                conn.setNetworkTimeout(TGroupDirectConnection.socketTimeoutExecutor, TRX_LOG_SOCKET_TIMEOUT);
                wrapWithLockWaitTimeout(conn, 3, () -> {
                    try {
                        if (FailPoint.isKeyEnable(FP_TRX_LOG_TB_FAILED_DURING_CREATE_TMP)
                            // Only one DN can create tmp table successfully.
                            && !lock.compareAndSet(false, true)) {
                            throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_FAIL,
                                "Fail point FP_TRX_LOG_TB_FAILED_DURING_CREATE_TMP");
                        }

                        stmt.execute(DROP_GLOBAL_TX_TABLE_V2_TMP);
                        stmt.execute(CREATE_GLOBAL_TX_TABLE_V2_TMP);
                    } catch (Exception e) {
                        EventLogger.log(EventType.TRX_LOG_ERR,
                            "Error during creating tmp table, caused by " + e.getMessage());
                        exceptions.offer(e);
                    }
                });
            } catch (Exception e) {
                exceptions.offer(e);
            }
        });
        AsyncUtils.waitAll(futures);

        if (!exceptions.isEmpty()) {
            exceptions.forEach(logger::error);
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG,
                "create tmp table failed " + exceptions.peek().getMessage());
        }
    }

    private static void switchTables(Set<String> dnIds, StringBuffer remark) {
        /*
        Normal procedure:
        A -> {A, tmp} -> {B, A} -> A
        In normal situation, we may face 2 cases:
        1. A, tmp (rename A to B, tmp to A)
        2. A, B (do nothing)
        The following cases are unexpected, and should not happen,
        but we still take them into consideration for safety.
        3. A (do nothing)
        4. A, B, tmp (do nothing, B will be dropped in the next step)
        5. B, tmp (rename tmp to A)
        6. tmp (rename tmp to A)
        7. B (create A)
        8. null (create A)
         */
        ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();
        AtomicBoolean lock = new AtomicBoolean(false);
        Collection<Future> futures = forEachDn(dnIds, (dnId) -> execWithLockWaitTimeout(dnId, exceptions, (stmt) -> {
            try {
                ResultSet rs = stmt.executeQuery(SHOW_ALL_GLOBAL_TX_TABLE_V2);
                boolean existsA = false, existsB = false, existsTmp = false;
                while (rs.next()) {
                    String tableName = String.format("`mysql`.`%s`", rs.getString(1));
                    if (SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE
                        .equalsIgnoreCase(tableName)) {
                        existsA = true;
                        remark.append("Exists A. ");
                    } else if (SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE_ARCHIVE
                        .equalsIgnoreCase(tableName)) {
                        existsB = true;
                        remark.append("Exists tmp. ");
                    } else if (SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE_TMP
                        .equalsIgnoreCase(tableName)) {
                        existsTmp = true;
                        remark.append("Exists B. ");
                    }
                }

                if (existsA && existsTmp && !existsB) {
                    // Case 1.
                    if (FailPoint.isKeyEnable(FP_TRX_LOG_TB_FAILED_DURING_SWITCH_TABLE)
                        // Only one DN switch table successfully.
                        && !lock.compareAndSet(false, true)) {
                        throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_FAIL,
                            "Fail point FP_TRX_LOG_TB_FAILED_DURING_SWITCH_TABLE");
                    }

                    stmt.execute(SWITCH_GLOBAL_TX_TABLE_V2);
                } else if (existsA) {
                    // Case 2, 3, 4: do nothing
                    EventLogger.log(EventType.TRX_LOG_ERR,
                        "Found unexpected log table status: (not exists tmp) or (exists A, B, tmp)");
                    remark.append("Do nothing: (not exists tmp) or (exists A, B, tmp). ");
                } else if (existsTmp) {
                    // Case 5 or Case 6.
                    EventLogger.log(EventType.TRX_LOG_ERR,
                        "Found unexpected log table status: (B, tmp) or (tmp), force rename tmp to A");
                    remark.append("Only rename tmp to A: (B, tmp) or (tmp) ");
                    stmt.execute(FORCE_RENAME_GLOBAL_TX_TABLE_V2);
                } else {
                    // Case 7 or Case 8.
                    EventLogger.log(EventType.TRX_LOG_ERR,
                        "Found unexpected log table status: (B) or (none), force create A");
                    remark.append("Create A: (B) or (none) ");
                    stmt.execute(CREATE_GLOBAL_TX_TABLE_V2);
                }
            } catch (Exception e) {
                EventLogger.log(EventType.TRX_LOG_ERR,
                    "Error during checking log table, caused by " + e.getMessage());
                exceptions.offer(e);
            }
        }));
        AsyncUtils.waitAll(futures);

        if (!exceptions.isEmpty()) {
            exceptions.forEach(remark::append);
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG, "switch tmp table failed " + exceptions.peek());
        }
    }

    private static void dropTable(Set<String> dnIds, StringBuffer remark, AtomicLong purged) {
        // Check support async commit variables.
        boolean dn57 = ExecutorContext.getContext(CDC_DB_NAME).getStorageInfoManager().supportAsyncCommit();
        // Max time 4 * 5 = 20s
        int retry = 0, maxRetry = 4, sleepSecond = 5;
        ConcurrentLinkedQueue<Exception> exceptions = new ConcurrentLinkedQueue<>();
        while (retry < maxRetry) {
            ITopologyExecutor executor = ExecutorContext.getContext(DEFAULT_DB_NAME).getTopologyExecutor();
            // Find min trx id for prepared trx.
            AtomicLong minTrxId = new AtomicLong(Long.MAX_VALUE);
            AtomicLong tmpMaxTrxId = new AtomicLong(Long.MIN_VALUE);
            scanRecoveredTrans(dnIds, executor, exceptions, minTrxId, tmpMaxTrxId);

            if (!exceptions.isEmpty()) {
                exceptions.forEach(remark::append);
                EventLogger.log(EventType.TRX_LOG_ERR, "Drop archive table failed when finding min trx id.");
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG,
                    "Drop archive table failed when finding min trx id. " + exceptions.peek());
            }

            // Find max trx id in archive table.
            AtomicLong maxTrxId = new AtomicLong(Long.MIN_VALUE);
            Collection<Future> futures =
                forEachDn(dnIds, (dnId) -> execWithLockWaitTimeout(dnId, exceptions, (stmt) -> {
                    try {
                        stmt.execute("begin");
                        if (dn57) {
                            // Use recover timestamp to see all records including uncommitted ones.
                            stmt.execute(String.format(SET_DISTRIBUTED_TRX_ID, 0));
                            stmt.execute(TrxLogTableConstants.RECOVER_TIMESTAMP_SQL);
                        }
                        ResultSet rs = stmt.executeQuery(SELECT_MAX_TX_ID_IN_ARCHIVE);
                        if (rs.next()) {
                            long txid = rs.getLong(1);
                            long tmp = maxTrxId.get();
                            while (txid > tmp && !maxTrxId.compareAndSet(tmp, txid)) {
                                tmp = maxTrxId.get();
                            }
                        }
                    } catch (SQLException e) {
                        if (e.getMessage().contains("doesn't exist")) {
                            // Ignore. Archive table is already dropped.
                        } else {
                            EventLogger.log(EventType.TRX_LOG_ERR,
                                "Error during finding max trx id in archive table, caused by " + e.getMessage());
                            exceptions.offer(e);
                        }
                    } finally {
                        try {
                            stmt.execute("rollback");
                        } catch (Throwable ignored) {
                        }
                    }
                }));
            AsyncUtils.waitAll(futures);

            if (!exceptions.isEmpty()) {
                exceptions.forEach(remark::append);
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG,
                    "Drop archive table failed when finding max trx id. " + exceptions.peek());
            }

            if (minTrxId.get() > maxTrxId.get()) {
                // Safe to delete archive table.
                AtomicBoolean lock = new AtomicBoolean(false);
                futures = forEachDn(dnIds, (dnId) -> execWithLockWaitTimeout(dnId, exceptions, (stmt) -> {
                    try {
                        ResultSet rs = stmt.executeQuery(SHOW_ALL_GLOBAL_TX_TABLE_V2);
                        boolean existsB = false;
                        while (rs.next()) {
                            String tableName = String.format("`mysql`.`%s`", rs.getString(1));
                            if (SystemTables.POLARDBX_GLOBAL_TX_LOG_TABLE_ARCHIVE.equalsIgnoreCase(tableName)) {
                                existsB = true;
                                break;
                            }
                        }
                        if (existsB) {
                            // Get approximate dropped rows.
                            rs = stmt.executeQuery(SELECT_TABLE_ROWS_V2);
                            if (rs.next()) {
                                purged.addAndGet(rs.getLong(1));
                            }

                            if (FailPoint.isKeyEnable(FP_TRX_LOG_TB_FAILED_DURING_DROP_TABLE)
                                // Only one DN drop table successfully.
                                && !lock.compareAndSet(false, true)) {
                                throw new TddlRuntimeException(ErrorCode.ERR_ASSERT_FAIL,
                                    "Fail point FP_TRX_LOG_TB_FAILED_DURING_DROP_TABLE");
                            }

                            // Then, drop table.
                            stmt.execute(DROP_GLOBAL_TX_TABLE_V2_ARCHIVE);
                        }
                    } catch (Exception e) {
                        exceptions.offer(e);
                        EventLogger.log(EventType.TRX_LOG_ERR,
                            "Error during dropping archive table, caused by " + e.getMessage());
                        logger.error("Drop archive table failed when dropping archive table.", e);
                    }
                }));
                AsyncUtils.waitAll(futures);

                if (!exceptions.isEmpty()) {
                    exceptions.forEach(remark::append);
                    throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG,
                        "Drop archive table failed when dropping archive table. " + exceptions.peek());
                }

                // Done.
                return;
            } else {
                remark.append("Found min prepared trx id ")
                    .append(minTrxId)
                    .append(" less than max archive trx id ")
                    .append(maxTrxId)
                    .append(", retry ")
                    .append(retry)
                    .append(". ");
            }

            try {
                Thread.sleep(sleepSecond * 1000);
            } catch (InterruptedException e) {
                remark.append("Interrupted when waiting for next retry, current retry ")
                    .append(retry)
                    .append(". ");
                throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG,
                    "Interrupted when waiting for next retry, current retry " + retry);
            }
            retry++;
        }
        throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG, "Max retry exceeds.");
    }

    private static Collection<Future> forEachDn(Set<String> dnIds, Consumer<String> task) {
        List<Future> futures = new ArrayList<>();
        ITopologyExecutor executor = ExecutorContext.getContext(DEFAULT_DB_NAME).getTopologyExecutor();
        for (String dnId : dnIds) {
            futures.add(executor.getExecutorService().submit(null, null,
                AsyncTask.build(() -> task.accept(dnId))));
        }
        return futures;
    }

    private static void execWithLockWaitTimeout(String dnId,
                                                ConcurrentLinkedQueue<Exception> exceptions,
                                                Consumer<Statement> task) {
        try (Connection conn = DbTopologyManager.getConnectionForStorage(dnId);
            Statement stmt = conn.createStatement()) {
            conn.setNetworkTimeout(TGroupDirectConnection.socketTimeoutExecutor, TRX_LOG_SOCKET_TIMEOUT);
            wrapWithLockWaitTimeout(conn, 3, () -> task.accept(stmt));
        } catch (SQLException e) {
            exceptions.offer(e);
        }
    }
}
