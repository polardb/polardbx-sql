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

import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.TransactionType;
import com.alibaba.polardbx.transaction.rawsql.RawSqlUtils;
import com.alibaba.polardbx.transaction.utils.XAUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.rpc.compatible.XStatement;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.alibaba.polardbx.transaction.TransactionState;
import com.alibaba.polardbx.transaction.jdbc.DeferredConnection;
import com.alibaba.polardbx.transaction.log.ConnectionContext;
import com.alibaba.polardbx.transaction.log.GlobalTxLog;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Task to scan prepared XA task with <pre>XA RECOVER</pre> command.
 *
 * @author Eric Fu
 */
public class XARecoverTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(XARecoverTask.class);

    private static final long RETRY_PERIOD = TimeUnit.SECONDS.toNanos(60);

    private final TransactionExecutor executor;

    private final Map<String, Set<PreparedXATrans>> lastPreparedSets = new HashMap<>();

    private final LoadingCache<Long, Long> badGroupUniqueIds = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.DAYS)
        .build(new CacheLoader<Long, Long>() {
            @Override
            public Long load(Long key) {
                return System.nanoTime();
            }
        });

    public XARecoverTask(TransactionExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void run() {
        final String schema = executor.getAsyncQueue().getSchema();
        boolean hasLeadership = ExecUtils.hasLeadership(schema);

        if (!hasLeadership) {
            logger.debug("Skip XA recovery task since I am not the leader");
            return;
        }

        try {
            final List<String> groupList = executor.getGroupList();
            final Set<String> groupSet = new HashSet<>(groupList);

            // Instance id (IP:PORT) -> DataSource (one of its group data sources)
            Map<String, IDataSource> instanceDataSources = new HashMap<>();
            for (String group : groupList) {
                // 每次都重新获取 IDataSource, 防重新加载
                TGroupDataSource dataSource = (TGroupDataSource) ExecutorContext.getContext(schema)
                    .getTopologyExecutor()
                    .getGroupExecutor(group).getDataSource();
                String instanceId = dataSource.getWriteInstanceId();
                instanceDataSources.putIfAbsent(instanceId, dataSource);
            }

            for (IDataSource dataSource : instanceDataSources.values()) {
                recoverInstance(dataSource, groupSet);
            }
        } catch (Throwable ex) {
            logger.error("Failed to check XA RECOVER transactions", ex);
        }
    }

    /**
     * Find the prepared transactions on MySQL with 'XA RECOVER' command
     *
     * @param dataSource Data Source of any group in this MySQL instance
     * @param groups All groups in this APPNAME, to filter out groups of other databases
     * @return a map from group name to prepared transactions on it
     */
    private void recoverInstance(IDataSource dataSource, Set<String> groups) {
        try (IConnection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("XA RECOVER");

            // Result set of XA RECOVER
            //
            // +----------+--------------+--------------+------------+
            // | formatID | gtrid_length | bqual_length | data       |
            // +----------+--------------+--------------+------------+
            // | 1        | 4            | 5            | txidgroup  |
            // +----------+--------------+--------------+------------+

            // group -> set of prepared trans entry
            Map<String, Set<PreparedXATrans>> preparedTrans = new HashMap<>();

            while (rs.next()) {
                long formatID = rs.getLong(1);
                int gtridLength = rs.getInt(2);
                int bqualLength = rs.getInt(3);
                byte[] data = rs.getBytes(4);

                // Filter out the records that cannot be parsed or not in current APPNAME
                XAUtils.XATransInfo transInfo = XAUtils.parseXid(formatID, gtridLength, bqualLength, data);
                if (transInfo != null && groups.contains(transInfo.getGroup())) {
                    PreparedXATrans entry = new PreparedXATrans(formatID, gtridLength, bqualLength, data);
                    preparedTrans.compute(transInfo.getGroup(), (gp, trans) -> {
                        if (trans == null) {
                            trans = new HashSet<>();
                        }
                        trans.add(entry);
                        return trans;
                    });
                }
            }
            rs.close();

            for (Map.Entry<String, Set<PreparedXATrans>> e : preparedTrans.entrySet()) {
                final String group = e.getKey();
                final Set<PreparedXATrans> lastPreparedSet = lastPreparedSets.get(group);
                final Set<PreparedXATrans> currentPreparedSet = e.getValue();

                // Roll back or forward transactions that appeared twice
                if (lastPreparedSet != null) {
                    Iterator<PreparedXATrans> iterator = currentPreparedSet.iterator();
                    while (iterator.hasNext()) {
                        PreparedXATrans next = iterator.next();
                        if (lastPreparedSet.contains(next)) {
                            if (rollBackOrForward(next, stmt)) {
                                iterator.remove();
                            }
                        }
                    }
                }

                lastPreparedSets.put(group, currentPreparedSet);
            }
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to do XA RECOVER", ex);
        }
    }

    private boolean rollBackOrForward(PreparedXATrans trans, Statement stmt) throws SQLException {
        XAUtils.XATransInfo transInfo =
            XAUtils.parseXid(trans.formatID, trans.gtridLength, trans.bqualLength, trans.data);
        assert transInfo != null; // already filtered

        final IServerConfigManager serverConfigManager = OptimizerHelper.getServerConfigManager();
        Pair<String, String> schemaAndGroup = serverConfigManager.findGroupByUniqueId(transInfo.primaryGroupUid);
        if (schemaAndGroup == null) {
            TransactionLogger.error(transInfo.transId, "Recovery failed: cannot find schema and group");
            long firstSeen = badGroupUniqueIds.getUnchecked(transInfo.primaryGroupUid);
            if (System.nanoTime() - firstSeen > RETRY_PERIOD) {
                // Cannot find schema and group for it. This is strange... Just roll it back
                TransactionLogger.warn(transInfo.transId, "rollback unknown XA transaction");
                return tryRollback(stmt, trans);
            }
            // Otherwise wait for a while since this group maybe not initialized yet
            return false;
        }
        final String schema = schemaAndGroup.getKey();
        final String primaryGroup = schemaAndGroup.getValue();

        GlobalTxLogManager txLog = TransactionManager.getInstance(schema).getGlobalTxLogManager();
        GlobalTxLog tx = txLog.get(primaryGroup, transInfo.transId);
        if (tx != null) {
            if (tx.getState() == TransactionState.ABORTED) {
                TransactionLogger.info(tx.getTxid(), "roll back XA transaction");
                return tryRollback(stmt, trans);
            } else {
                // This transaction is committed. Roll it forward
                if (tx.getType() == TransactionType.TSO) {
                    assert tx.getCommitTimestamp() != null : "TSO transaction need commit timestamp";
                    TransactionLogger.info(tx.getTxid(), "roll forward TSO transaction");
                    return tryCommitTSO(stmt, trans, tx.getCommitTimestamp());
                } else if (tx.getType() == TransactionType.XA) {
                    TransactionLogger.info(tx.getTxid(), "roll forward XA transaction");
                    return tryCommitXA(stmt, trans);
                } else {
                    throw new AssertionError();
                }
            }
        } else {
            // Transaction was not committed nor aborted
            // Write transaction log to the primary group of this transaction
            IDataSource dataSource = txLog.getTransactionExecutor()
                .getGroupExecutor(primaryGroup)
                .getDataSource();

            final IConnection conn2 = dataSource.getConnection();
            conn2.setAutoCommit(false);
            try {
                txLog.append(transInfo.transId, TransactionType.XA, TransactionState.ABORTED, new ConnectionContext(),
                    conn2);
                // Successfully set the transaction to ABORT state, so roll it back
                TransactionLogger.info(transInfo.transId, "Abort XA transaction and roll back");
                stmt.execute("XA ROLLBACK " + trans.toXid());

                conn2.commit();
                return true;
            } catch (SQLIntegrityConstraintViolationException e) {
                // Someone else has handled this, so do nothing
                conn2.rollback();
                return false;
            } catch (SQLException ex) {
                TransactionLogger
                    .info(transInfo.transId, "XA ROLLBACK error: {0} {1}", ex.getMessage(), transInfo.toXidString());

                if (ex.getErrorCode() == com.alibaba.polardbx.ErrorCode.ER_XAER_RMFAIL) {
                    return true; // Maybe not prepared yet. Ignore such exceptions
                } else if (ex.getErrorCode() == com.alibaba.polardbx.ErrorCode.ER_XAER_NOTA) {
                    conn2.rollback();
                    return true; // Transaction still running or recovered by others
                } else {
                    return false;
                }
            } finally {
                conn2.setAutoCommit(true);
                conn2.close();
            }
        }
    }

    private static boolean tryRollback(Statement stmt, PreparedXATrans trans) {
        try {
            stmt.execute("XA ROLLBACK " + trans.toXid());
            return true;
        } catch (SQLException ex) {
            if (ex.getErrorCode() == com.alibaba.polardbx.ErrorCode.ER_XAER_RMFAIL) {
                return true; // Maybe not prepared yet. Ignore such exceptions
            } else if (ex.getErrorCode() == com.alibaba.polardbx.ErrorCode.ER_XAER_NOTA) {
                return true; // Transaction lost or recovered by others
            }
            TransactionLogger.getLogger().error("XA ROLLBACK error", ex);
            return false;
        }
    }

    private static boolean tryCommitXA(Statement stmt, PreparedXATrans trans) {
        return tryCommit0(stmt, "XA COMMIT " + trans.toXid());
    }

    private static boolean tryCommitTSO(Statement stmt, PreparedXATrans trans, long commitTimestamp)
        throws SQLException {
        final XConnection xConnection;
        if (stmt.isWrapperFor(XStatement.class) &&
            (xConnection = stmt.getConnection().unwrap(XConnection.class)).supportMessageTimestamp()) {
            if (stmt.getConnection().isWrapperFor(DeferredConnection.class)) {
                stmt.getConnection().unwrap(DeferredConnection.class).flushUnsent();
            }
            xConnection.setLazyCommitSeq(commitTimestamp);
            return tryCommit0(stmt, "XA COMMIT " + trans.toXid());
        }
        return tryCommit0(stmt, "SET innodb_commit_seq = " + commitTimestamp + "; XA COMMIT " + trans.toXid());
    }

    private static boolean tryCommit0(Statement stmt, String sql) {
        try {
            stmt.execute(sql);
            return true;
        } catch (SQLException ex) {
            if (ex.getErrorCode() == com.alibaba.polardbx.ErrorCode.ER_XAER_RMFAIL) {
                return true; // Maybe not prepared yet. Ignore such exceptions
            } else if (ex.getErrorCode() == com.alibaba.polardbx.ErrorCode.ER_XAER_NOTA) {
                return true; // Transaction lost or recovered by others
            }
            TransactionLogger.getLogger().error("XA COMMIT error", ex);
            return false;
        }
    }

    /**
     * PreparedXATrans represents one row in the result set of XA RECOVER
     */
    private static class PreparedXATrans {

        final long formatID;
        final int gtridLength;
        final int bqualLength;
        final byte[] data;

        public PreparedXATrans(long formatID, int gtridLength, int bqualLength, byte[] data) {
            this.formatID = formatID;
            this.gtridLength = gtridLength;
            this.bqualLength = bqualLength;
            this.data = data;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PreparedXATrans that = (PreparedXATrans) o;
            return formatID == that.formatID && gtridLength == that.gtridLength && bqualLength == that.bqualLength
                && Arrays.equals(data, that.data);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(formatID, gtridLength, bqualLength);
            result = 31 * result + Arrays.hashCode(data);
            return result;
        }

        public String toXid() {
            StringBuilder builder = new StringBuilder();
            RawSqlUtils.formatParameter(Arrays.copyOfRange(data, 0, gtridLength), builder);
            builder.append(',');
            RawSqlUtils.formatParameter(Arrays.copyOfRange(data, gtridLength, gtridLength + bqualLength), builder);
            builder.append(',');
            builder.append(formatID);
            return builder.toString();
        }
    }
}
