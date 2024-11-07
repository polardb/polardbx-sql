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

package com.alibaba.polardbx.transaction.async;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.TransactionState;
import com.alibaba.polardbx.transaction.log.ConnectionContext;
import com.alibaba.polardbx.transaction.log.GlobalTxLog;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;
import com.alibaba.polardbx.transaction.log.RedoLog;
import com.alibaba.polardbx.transaction.log.RedoLogManager;
import com.alibaba.polardbx.transaction.utils.MySQLErrorUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Task to scan prepared BestEffortTransaction task by checking <pre>__drds_redo_log</pre> table.
 *
 * @author Eric Fu
 */
public class ScanBestEffortPreparedTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ScanBestEffortPreparedTask.class);

    private static final long RETRY_PERIOD = TimeUnit.SECONDS.toNanos(60);

    private final TransactionExecutor executor;
    private final GlobalTxLogManager globalTxLogManager;

    private Map<String, Set<PreparedBestEffortTrans>> lastPreparedSets = new HashMap<>();

    private final LoadingCache<Long, Long> badGroupUniqueIds = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.DAYS)
        .build(new CacheLoader<Long, Long>() {
            @Override
            public Long load(Long key) {
                return System.nanoTime();
            }
        });

    public ScanBestEffortPreparedTask(TransactionExecutor executor, GlobalTxLogManager globalTxLogManager) {
        this.executor = executor;
        this.globalTxLogManager = globalTxLogManager;
    }

    @Override
    public void run() {
        boolean hasLeadership = ExecUtils.hasLeadership(executor.getAsyncQueue().getSchema());

        if (!hasLeadership) {
            logger.debug("Skip 2PC recovery task since I am not the leader");
            return;
        }

        try {
            for (String group : executor.getGroupList()) {

                Set<PreparedBestEffortTrans> lastPreparedSet = lastPreparedSets.get(group);
                if (lastPreparedSet == null) {
                    lastPreparedSet = new HashSet<>();
                }

                // 注意: 这里每次都重新获取 IDataSource, 防重新加载。
                IDataSource dataSource = executor.getGroupExecutor(group).getDataSource();
                lastPreparedSet = scanGroup(group, dataSource, lastPreparedSet);
                lastPreparedSets.put(group, lastPreparedSet);
            }
        } catch (Throwable ex) {
            // Just in case
            logger.error("Failed to scan best-effort transactions", ex);
        }
    }

    private Set<PreparedBestEffortTrans> scanGroup(String group, IDataSource dataSource,
                                                   Set<PreparedBestEffortTrans> lastPreparedSet) {
        try (IConnection conn = dataSource.getConnection()) {
            Set<PreparedBestEffortTrans> preparedTrans = RedoLogManager.queryPreparedTrans(conn);
            Iterator<PreparedBestEffortTrans> iterator = preparedTrans.iterator();
            while (iterator.hasNext()) {
                PreparedBestEffortTrans next = iterator.next();
                if (lastPreparedSet.contains(next)) {
                    if (rollBackOrForward(next, conn)) {
                        iterator.remove();
                    }
                }
            }
            return preparedTrans;
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to query best-effort transactions on group " + group, ex);
        }
    }

    private boolean rollBackOrForward(PreparedBestEffortTrans trans, IConnection conn) throws SQLException {
        final IServerConfigManager serverConfigManager = OptimizerHelper.getServerConfigManager();
        Pair<String, String> schemaAndGroup =
            serverConfigManager.findGroupByUniqueId(trans.primaryGroupUid, new HashMap<>());
        if (schemaAndGroup == null) {
            TransactionLogger.error(trans.transId, "Recovery failed: cannot find schema and group");
            long firstSeen = badGroupUniqueIds.getUnchecked(trans.primaryGroupUid);
            if (System.nanoTime() - firstSeen > RETRY_PERIOD) {
                // Cannot find schema and group for it. This is strange... Just roll it back
                TransactionLogger.warn(trans.transId, "Roll back unknown best-effort transaction");
                return tryRollback(trans.transId, conn);
            }
            // Otherwise wait for a while since this group maybe not initialized yet
            return false;
        }
        final String schema = schemaAndGroup.getKey();
        final String primaryGroup = schemaAndGroup.getValue();

        GlobalTxLogManager txLog = TransactionManager.getInstance(schema).getGlobalTxLogManager();
        GlobalTxLog tx = txLog.getWithTimeout(primaryGroup, trans.transId);
        if (tx != null) {
            if (tx.getState() == TransactionState.ABORTED) {
                TransactionLogger.warn(tx.getTxid(), "Roll back best-effort transaction");
                return tryRollback(trans.transId, conn);
            } else {
                // This transaction is committed. Roll it forward
                TransactionLogger.warn(tx.getTxid(), "Roll forward best-effort transaction");
                try {
                    tryCommit(trans.transId, conn, tx.getContext());
                } catch (SQLException ex) {
                    if (MySQLErrorUtils.isTransient(ex)) {
                        // Retry later
                        return false;
                    } else {
                        // Cannot resolve it by retrying so rollback (e.g. insert conflict)
                        // The error message should be notified to user so that subsequent revise could be taken
                        TransactionLogger.error(tx.getTxid(),
                            "Failed to roll forward best-effort transaction: " + ex.getMessage());
                        tryRollback(trans.transId, conn);
                    }
                }
                return true;
            }
        } else {
            // Transaction was not committed nor aborted
            // Write transaction log to the primary group of this transaction
            IDataSource dataSource = txLog.getTransactionExecutor().getGroupExecutor(primaryGroup).getDataSource();
            try (IConnection conn2 = dataSource.getConnection()) {
                GlobalTxLogManager.appendWithSocketTimeout(trans.transId, TransactionType.BED, TransactionState.ABORTED,
                    new ConnectionContext(), conn2);
                // Successfully set the transaction to ABORT state, so roll it back
                TransactionLogger.warn(trans.transId, "Abort best-effort transaction and roll back");
                return tryRollback(trans.transId, conn);
            } catch (SQLIntegrityConstraintViolationException e) {
                // Someone else has handled this, so do nothing
                return false;
            }
        }
    }

    private static boolean tryRollback(long txid, IConnection conn) {
        RedoLogManager.clean(txid, conn);
        return true;
    }

    private static void tryCommit(long txid, IConnection conn, ConnectionContext context) throws SQLException {
        try {
            conn.setAutoCommit(false);
            conn.setEncoding("utf8mb4");
            conn.setServerVariables(context.getVariables());

            // Query and lock all redo-logs via 'SELECT FOR UPDATE'
            List<RedoLog> redoLogs = RedoLogManager.queryRedoLogs(txid, conn);

            // Then delete them
            RedoLogManager.clean(txid, conn);

            if (!redoLogs.isEmpty()) {
                TransactionLogger.debug(txid, "Executing redo-log for trans");

                try (Statement stmt = conn.createStatement()) {
                    for (RedoLog redoLog : redoLogs) {
                        TransactionLogger.warn(txid, "Execute redo-log: " + redoLog.getInfo());
                        stmt.addBatch(redoLog.getInfo());
                    }
                    stmt.executeBatch();
                }
            }
            conn.commit();
        } catch (SQLException ex) {
            try {
                conn.rollback();
            } catch (SQLException ex2) {
                // ignore
            }
            throw ex;
        } finally {
            conn.setAutoCommit(true);
        }
    }

    /**
     * PreparedBestEffortTrans represents one row in the `__drds_redo_log` table
     */
    public static class PreparedBestEffortTrans {
        final long transId;
        final long primaryGroupUid;

        public PreparedBestEffortTrans(long transId, long primaryGroupUid) {
            this.transId = transId;
            this.primaryGroupUid = primaryGroupUid;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PreparedBestEffortTrans that = (PreparedBestEffortTrans) o;
            return transId == that.transId && primaryGroupUid == that.primaryGroupUid;
        }

        @Override
        public int hashCode() {
            return Objects.hash(transId, primaryGroupUid);
        }
    }
}
