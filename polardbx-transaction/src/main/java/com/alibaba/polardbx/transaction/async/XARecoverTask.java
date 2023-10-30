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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.alibaba.polardbx.rpc.compatible.XStatement;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.TransactionState;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.transaction.TsoTransaction;
import com.alibaba.polardbx.transaction.jdbc.DeferredConnection;
import com.alibaba.polardbx.transaction.log.ConnectionContext;
import com.alibaba.polardbx.transaction.log.GlobalTxLog;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;
import com.alibaba.polardbx.transaction.rawsql.RawSqlUtils;
import com.alibaba.polardbx.transaction.utils.XAUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.commons.collections.CollectionUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.executor.utils.ExecUtils.getInstId2GroupList;
import static com.alibaba.polardbx.transaction.TsoTransaction.SET_DISTRIBUTED_TRX_ID;
import static com.alibaba.polardbx.transaction.TsoTransaction.SET_REMOVE_DISTRIBUTED_TRX;

/**
 * Task to scan prepared XA task with <pre>XA RECOVER</pre> command.
 *
 * @author Eric Fu
 */
public class XARecoverTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(XARecoverTask.class);

    private static final long RETRY_PERIOD = TimeUnit.SECONDS.toNanos(60 * 60);

    /**
     * When we encounter a branch with this gtrid, it is an XA trx created by recover task.
     * We can commit that branch, and it would not cause anything inconsistent.
     * It looks like: "POLARDB-X-RECOVER-TASK@{trx-id}, {bqual}, 2"
     */
    public static final String RECOVER_GTRID_PREFIX = "POLARDB-X-RECOVER-TASK";

    /**
     * Record all under-processing transactions.
     * Map: transaction id -> schema name of recover task which is processing this transaction.
     */
    private static final ConcurrentHashMap<Long, String> processingTrans = new ConcurrentHashMap<>();

    private final String schema;

    private final TransactionExecutor executor;

    private final Map<String, Set<PreparedXATrans>> lastPreparedSets = new HashMap<>();

    private final Map<String, List<String>> schemaAndGroupsCache = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    private final boolean supportAsyncCommit;

    private final LoadingCache<Long, Long> badGroupUniqueIds = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.DAYS)
        .build(new CacheLoader<Long, Long>() {
            @Override
            public Long load(Long key) {
                return System.nanoTime();
            }
        });

    public XARecoverTask(TransactionExecutor executor, boolean supportAsyncCommit) {
        this.executor = executor;
        this.schema = executor.getAsyncQueue().getSchema();
        this.supportAsyncCommit = supportAsyncCommit;
    }

    @Override
    public void run() {
        boolean hasLeadership = ExecUtils.hasLeadership(schema);

        if (!hasLeadership) {
            logger.debug("Skip XA recovery task since I am not the leader");
            return;
        }

        if (!InstConfUtil.getBool(ConnectionParams.ENABLE_TRANSACTION_RECOVER_TASK)) {
            return;
        }

        final Map savedMdcContext = MDC.getCopyOfContextMap();
        try {
            MDC.put(MDC.MDC_KEY_APP, schema.toLowerCase());
            final List<String> groupList = executor.getGroupList();
            final Set<String> groupSet = new HashSet<>(groupList.size());
            groupList.stream().map(XAUtils::uniqueGroupForBqual).forEach(groupSet::add);

            // Instance id (IP:PORT) -> DataSource (one of its group data sources)
            Map<String, IDataSource> instanceDataSources = new HashMap<>();
            for (String group : groupList) {
                // 每次都重新获取 IDataSource, 防重新加载
                TGroupDataSource dataSource = (TGroupDataSource) ExecutorContext.getContext(schema)
                    .getTopologyExecutor()
                    .getGroupExecutor(group).getDataSource();
                String instanceId = dataSource.getMasterSourceAddress();
                instanceDataSources.putIfAbsent(instanceId, dataSource);
            }

            for (IDataSource dataSource : instanceDataSources.values()) {
                recoverInstance(dataSource, groupSet);
            }

            TransactionManager.getInstance(schema).setFirstRecover(false);
        } catch (Throwable ex) {
            logger.error("Failed to check XA RECOVER transactions", ex);
        } finally {
            MDC.setContextMap(savedMdcContext);
        }
    }

    /**
     * Find the prepared transactions on MySQL with 'XA RECOVER' command
     *
     * @param dataSource Data Source of any group in this MySQL instance
     * @param groups All groups in this APPNAME, to filter out groups of other databases
     */
    private void recoverInstance(IDataSource dataSource, Set<String> groups) {
        // Cache the schema and groups mapping generated in this round.
        schemaAndGroupsCache.clear();
        try (IConnection conn = new DeferredConnection(dataSource.getConnection(),
            InstConfUtil.getBool(ConnectionParams.USING_RDS_RESULT_SKIP));
            Statement stmt = conn.createStatement()) {
            if (conn.isWrapperFor(XConnection.class)) {
                // Note: XA RECOVER will hold the LOCK_transaction_cache lock, so never block it.
                conn.unwrap(XConnection.class).setDefaultTokenKb(Integer.MAX_VALUE);
            }
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
            // Transactions generated by recover task.
            List<PreparedXATrans> recoverTrans = new ArrayList<>();

            while (rs.next()) {
                long formatID = rs.getLong(1);
                int gtridLength = rs.getInt(2);
                int bqualLength = rs.getInt(3);
                byte[] data = rs.getBytes(4);

                // Filter out the records that cannot be parsed or not in current APPNAME
                XAUtils.XATransInfo transInfo = XAUtils.parseXid(formatID, gtridLength, bqualLength, data);
                if (null == transInfo) {
                    continue;
                }

                final IServerConfigManager serverConfigManager = OptimizerHelper.getServerConfigManager();
                Pair<String, String> schemaAndGroup =
                    serverConfigManager.findGroupByUniqueId(transInfo.primaryGroupUid, schemaAndGroupsCache);

                if (2 == transInfo.formatId) {
                    recoverTrans.add(new PreparedXATrans(formatID, gtridLength, bqualLength, data));
                }

                if (1 == transInfo.formatId && (groups.contains(transInfo.getGroup()) || null == schemaAndGroup)) {
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

            handleRecoverTrans(stmt, recoverTrans);
            try {
                handlePreparedTrans(stmt, preparedTrans);
            } finally {
                if (supportAsyncCommit) {
                    // Clean up some session variables set during handlePreparedTrans.
                    stmt.execute("ROLLBACK");
                }
            }
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to do XA RECOVER", ex);
        }
    }

    /**
     * Handle dangling transactions generated by recover task.
     * These transactions append an aborted log into log table. and always can be committed.
     * The worst case is that some transactions, which should be committed but not yet cross
     * the commit point, are rolled back eventually, which does not harm the ACID of trx.
     * Such case only happens when CN crashes during a recover task.
     *
     * @param stmt statement used to execute XA COMMIT.
     * @param recoverTrans dangling transactions generated by recover task.
     */
    private void handleRecoverTrans(Statement stmt, List<PreparedXATrans> recoverTrans) throws SQLException {
        for (PreparedXATrans recoverTran : recoverTrans) {
            tryCommitXA(stmt, recoverTran);
        }
    }

    /**
     * Handle prepared transactions found by this round of recover task.
     *
     * @param stmt statement used to execute XA COMMIT/ROLLBACK.
     * @param preparedTrans prepared transactions to be handled.
     */
    private void handlePreparedTrans(Statement stmt, Map<String, Set<PreparedXATrans>> preparedTrans)
        throws SQLException {
        TransactionManager txMgr = TransactionManager.getInstance(schema);
        for (Map.Entry<String, Set<PreparedXATrans>> e : preparedTrans.entrySet()) {
            final String group = e.getKey();
            final Set<PreparedXATrans> lastPreparedSet = lastPreparedSets.get(group);
            final Set<PreparedXATrans> currentPreparedSet = e.getValue();

            if (txMgr.isFirstRecover() || lastPreparedSet != null) {
                Iterator<PreparedXATrans> iterator = currentPreparedSet.iterator();
                while (iterator.hasNext()) {
                    PreparedXATrans trans = iterator.next();
                    XAUtils.XATransInfo transInfo =
                        XAUtils.parseXid(trans.formatID, trans.gtridLength, trans.bqualLength, trans.data);
                    assert transInfo != null;
                    final IServerConfigManager serverConfigManager = OptimizerHelper.getServerConfigManager();
                    Pair<String, String> schemaAndGroup =
                        serverConfigManager.findGroupByUniqueId(transInfo.primaryGroupUid, schemaAndGroupsCache);
                    if (txMgr.isFirstRecover() || lastPreparedSet.contains(trans) || null == schemaAndGroup) {
                        // Roll back or forward transactions if
                        // 1. this prepared trx appeared twice and belongs to this schema,
                        // 2. or this is the first round of recover task,
                        // 3. or the primary schema is not found and this trx should be rolled back.
                        String currentProcessingSchema = processingTrans.putIfAbsent(transInfo.transId, schema);
                        if (null == currentProcessingSchema || schema.equals(currentProcessingSchema)) {
                            try {
                                logger.warn(schema + ": Process dangling trx: " + transInfo.toXidString());
                                if (rollBackOrForward(trans, stmt, transInfo, schemaAndGroup)) {
                                    iterator.remove();
                                }
                            } finally {
                                processingTrans.remove(transInfo.transId);
                            }
                        } else {
                            // Other thread is processing this transaction.
                            logger.warn(schema + ": Ignore dangling trx since other thread is processing it: "
                                + transInfo.toXidString());
                            iterator.remove();
                        }
                    }
                }
            }

            lastPreparedSets.put(group, currentPreparedSet);
        }
    }

    /**
     * @return true if we succeed processing this dangling transaction.
     */
    private boolean rollBackOrForward(PreparedXATrans trans, Statement stmt, XAUtils.XATransInfo transInfo,
                                      Pair<String, String> schemaAndGroup) throws SQLException {
        logger.info("[Recover] Processing transaction: " + transInfo.toXidString());
        if (schemaAndGroup == null) {
            TransactionLogger.error(transInfo.transId,
                "[Recover] Recovery: cannot find schema and group, rollback unknown XA transaction");
            return tryRollback(stmt, trans);
        } else if (null == schemaAndGroup.getValue()) {
            TransactionLogger.error(transInfo.transId, "Recovery failed: group not inited");
            long firstSeen = badGroupUniqueIds.getUnchecked(transInfo.primaryGroupUid);
            if (InstConfUtil.getBool(ConnectionParams.ROLLBACK_UNKNOWN_XA_TRANSACTION)
                && System.nanoTime() - firstSeen > RETRY_PERIOD) {
                // In 1 hour this schema is still not inited, rollback this trx.
                TransactionLogger.warn(transInfo.transId, "rollback unknown XA transaction");
                Optional.ofNullable(OptimizerContext.getTransStat(schema))
                    .ifPresent(s -> s.countRecoverRollback.incrementAndGet());
                return tryRollback(stmt, trans);
            }
            // Otherwise wait for a while since this group maybe not initialized yet
            return false;
        }

        final String schema = schemaAndGroup.getKey();
        final String primaryGroup = schemaAndGroup.getValue();

        GlobalTxLogManager primaryGroupTxLogMgr = TransactionManager.getInstance(schema).getGlobalTxLogManager();
        GlobalTxLog txLog = primaryGroupTxLogMgr.get(primaryGroup, transInfo, schema);
        if (txLog != null) {
            /*
            Case 1: Trx log is found.
             */
            if (txLog.getState() == TransactionState.ABORTED) {
                /*
                Case 1.1: Trx is marked as aborted. Roll it back.
                 */
                Optional.ofNullable(OptimizerContext.getTransStat(schema))
                    .ifPresent(s -> s.countRecoverRollback.incrementAndGet());
                String info = "[Recover] roll back XA transaction " + transInfo.toXidString();
                logger.warn(info);
                TransactionLogger.warn(txLog.getTxid(), info);
                return tryRollback(stmt, trans);
            } else if (txLog.getState() == TransactionState.SUCCEED) {
                /*
                Case 1.2: Trx is marked as committed. Roll it forward.
                 */
                Optional.ofNullable(OptimizerContext.getTransStat(schema))
                    .ifPresent(s -> s.countRecoverCommit.incrementAndGet());
                if (txLog.getType() == TransactionType.TSO) {
                    assert txLog.getCommitTimestamp() != null : "TSO transaction need commit timestamp";
                    String info = "roll forward TSO transaction " + transInfo.toXidString();
                    logger.warn(info);
                    TransactionLogger.warn(txLog.getTxid(), info);
                    return tryCommitTSO(stmt, trans, txLog.getCommitTimestamp(), transInfo.transId, supportAsyncCommit);
                } else if (txLog.getType() == TransactionType.XA) {
                    TransactionLogger.warn(txLog.getTxid(), "roll forward XA transaction");
                    return tryCommitXA(stmt, trans);
                } else {
                    throw new AssertionError();
                }
            } else if (txLog.getState() == TransactionState.ATTACHED) {
                // Not a dangling trx.
                return true;
            } else {
                throw new AssertionError("Bad state: " + txLog.getState());
            }
        } else {
            /*
            Case 2: Trx log is not found in primary group.
            Note, that it may be an async commit transaction.
             */

            // Check whether it is an async commit transaction.
            Map<String, List<TGroupDataSource>> allDn = getInstId2GroupList(schema);
            List<TGroupDataSource> noLogDataSources = new ArrayList<>();
            int expected = 0, found = 0;
            boolean abort = false;
            long commitTimeStamp = 0;
            for (List<TGroupDataSource> groupDataSources : allDn.values()) {
                assert CollectionUtils.isNotEmpty(groupDataSources);
                // Since all data sources are in the same DN, any data source is ok.
                final TGroupDataSource groupDataSource = groupDataSources.get(0);

                // Get async commit trx log from this DN.
                GlobalTxLog asyncCommitTxLog =
                    GlobalTxLogManager.getAsyncCommitLog(groupDataSource, transInfo.transId, schema);

                if (null == asyncCommitTxLog) {
                    noLogDataSources.add(groupDataSource);
                } else {
                    logger.info("[Async Commit][Recover] Found async commit log: " + asyncCommitTxLog);
                    TransactionLogger.warn(transInfo.transId, "[Recover] Found async commit log: " + asyncCommitTxLog);

                    if (asyncCommitTxLog.getState() == TransactionState.ABORTED) {
                        abort = true;
                        continue;
                    }

                    if (0 == expected) {
                        expected = asyncCommitTxLog.getParticipants();
                    }
                    commitTimeStamp = Long.max(commitTimeStamp, asyncCommitTxLog.getCommitTimestamp());

                    found++;
                }
            }

            if (!supportAsyncCommit || abort || expected != found || 0 == found) {
                /*
                Case 2.1: It must be one of the following cases, and should be rolled back.
                a. It is an async commit transaction, and it is marked as aborted;
                b. It is an async commit transaction, and not all of its branches are prepared;
                c. It did not write any async commit log, neither normal commit log;
                d. Async commit is not supported, and normal commit log are not written.
                 */
                Optional.ofNullable(OptimizerContext.getTransStat(schema))
                    .ifPresent(s -> s.countRecoverRollback.incrementAndGet());
                return appendLogAndRollback(trans, stmt, transInfo, primaryGroupTxLogMgr, primaryGroup,
                    noLogDataSources);
            } else {
                /*
                Case 2.2: It is an async commit transaction, and we found all expected trx logs,
                and each trx log indicates this trx should be committed.
                 */
                if (commitTimeStamp <= 0 || !TsoTransaction.isMinCommitSeq(commitTimeStamp)) {
                    String error = "[Async Commit][Recover] found bad commit_seq: " + commitTimeStamp + " for "
                        + transInfo.transId;
                    TransactionLogger.error(transInfo.transId, error);
                    throw new TddlRuntimeException(ErrorCode.ERR_TRANS, error);
                }
                Optional.ofNullable(OptimizerContext.getTransStat(schema))
                    .ifPresent(s -> s.countRecoverCommit.incrementAndGet());
                return tryCommitTSO(stmt, trans, TsoTransaction.convertFromMinCommitSeq(commitTimeStamp),
                    transInfo.transId, supportAsyncCommit);
            }
        }
    }

    private boolean appendLogAndRollback(PreparedXATrans trans, Statement stmt, XAUtils.XATransInfo transInfo,
                                         GlobalTxLogManager primaryGroupTxLogMgr, String primaryGroup,
                                         List<TGroupDataSource> noLogDataSources)
        throws SQLException {
        List<Pair<IConnection, String>> logConns = new ArrayList<>(noLogDataSources.size() + 1);
        try {
            /*
            Step 1, append an aborted trx log to each DN's async commit trx log table if async commit is supported.
            Step 2, append an aborted trx log to primary-group's normal trx log table.
            Step 3, execute XA ROLLBACK to rollback the branch.
            Note, that any step fails, all succeeded steps should be rolled back.
            Hence, we use an extra XA trx to achieve such atomicity. POLARDB-X-RECOVER-TASK@{trx-id}, {bqual}, 2
            */
            final long trxId = transInfo.transId;
            int i = 0;
            if (supportAsyncCommit) {
                for (TGroupDataSource dataSource : noLogDataSources) {
                    final IConnection conn = dataSource.getConnection();
                    final String xid = getRecoverXid(trxId, "async-commit-" + i++);
                    logConns.add(new Pair<>(conn, xid));
                }

                for (Pair<IConnection, String> logConn : logConns) {
                    try {
                        logConn.getKey().executeLater("xa begin " + logConn.getValue());
                        GlobalTxLogManager.appendAsyncCommitLog(trxId, 0, logConn.getKey());
                    } catch (Throwable t) {
                        logger.error("Append aborted log to async commit log table failed", t);
                        rollbackLogConns(logConns);
                        return true;
                    }
                }
            }

            IDataSource dataSource = primaryGroupTxLogMgr.getTransactionExecutor()
                .getGroupExecutor(primaryGroup)
                .getDataSource();

            final IConnection conn = dataSource.getConnection();
            final String xid = getRecoverXid(trxId, "normal-commit-" + i++);
            logConns.add(new Pair<>(conn, xid));

            try {
                conn.executeLater("xa begin " + xid);
                GlobalTxLogManager txLog = TransactionManager.getInstance(schema).getGlobalTxLogManager();
                txLog.append(transInfo.transId, TransactionType.XA, TransactionState.ABORTED, new ConnectionContext(),
                    conn);
            } catch (Throwable t) {
                logger.error("Append aborted log to normal commit log table failed", t);
                rollbackLogConns(logConns);
                return false;
            }

            try {
                prepareLogConns(logConns);
            } catch (Throwable t) {
                logger.error("Append aborted log to normal commit log table failed", t);
                rollbackLogConns(logConns);
                return false;
            }

            // Safe to perform xa rollback now.
            try {
                if (supportAsyncCommit) {
                    try {
                        stmt.execute(String.format(SET_DISTRIBUTED_TRX_ID, transInfo.transId));
                        stmt.execute(SET_REMOVE_DISTRIBUTED_TRX);
                    } catch (SQLException e) {
                        logger.warn("Set async commit info failed.", e);
                    }
                }

                stmt.execute("XA ROLLBACK " + trans.toXid());
                logger.info("[Async Commit][Recover] roll back TSO transaction " + transInfo.transId);
                TransactionLogger.warn(transInfo.transId, "[Async Commit][Recover] roll back TSO transaction");
            } catch (SQLException ex) {
                logger.info("XA ROLLBACK error: " + ex.getMessage());
                TransactionLogger
                    .warn(transInfo.transId, "XA ROLLBACK error: {0} {1}", ex.getMessage(), trans.toXid());

                if (ex.getErrorCode() == ErrorCode.ER_XAER_RMFAIL.getCode()) {
                    rollbackLogConns(logConns);
                    return true; // Maybe not prepared yet. Ignore such exceptions
                } else if (ex.getErrorCode() == ErrorCode.ER_XAER_NOTA.getCode()) {
                    rollbackLogConns(logConns);
                    return true; // Transaction still running or recovered by others
                } else {
                    rollbackLogConns(logConns);
                    return false;
                }
            }

            commitLogConns(logConns);
        } finally {
            closeLogConns(logConns);
        }
        return true;
    }

    private void closeLogConns(List<Pair<IConnection, String>> logConns) {
        for (Pair<IConnection, String> logConn : logConns) {
            try {
                if (!logConn.getKey().isClosed()) {
                    logConn.getKey().close();
                }
            } catch (Throwable t) {
                logger.warn("[RECOVER] Close log connections failed.", t);
            }
        }
    }

    private void rollbackLogConns(List<Pair<IConnection, String>> logConns) {
        for (Pair<IConnection, String> logConn : logConns) {
            try (Statement stmt = logConn.getKey().createStatement()) {
                stmt.execute("xa end " + logConn.getValue());
            } catch (Throwable t) {
                logger.warn("[Recover] xa end failed", t);
            }

            try (Statement stmt = logConn.getKey().createStatement()) {
                stmt.execute("xa rollback " + logConn.getValue());
            } catch (Throwable t) {
                logger.warn("[Recover] xa rollback failed", t);
                logConn.getKey().discard(t);
            }
        }
    }

    private void prepareLogConns(List<Pair<IConnection, String>> logConns) {
        for (Pair<IConnection, String> logConn : logConns) {
            try (Statement stmt = logConn.getKey().createStatement()) {
                stmt.execute("xa end " + logConn.getValue());
                stmt.execute("xa prepare " + logConn.getValue());
            } catch (Throwable t) {
                logger.warn("[Recover] xa prepare failed", t);
                logConn.getKey().discard(t);
            }
        }
    }

    private void commitLogConns(List<Pair<IConnection, String>> logConns) {
        for (Pair<IConnection, String> logConn : logConns) {
            try (Statement stmt = logConn.getKey().createStatement()) {
                stmt.execute("xa commit " + logConn.getValue());
            } catch (Throwable t) {
                logger.warn("[Recover] xa commit failed", t);
                logConn.getKey().discard(t);
            }
        }
    }

    private static boolean tryRollback(Statement stmt, PreparedXATrans trans) {
        try {
            stmt.execute("XA ROLLBACK " + trans.toXid());
            return true;
        } catch (SQLException ex) {
            if (ex.getErrorCode() == ErrorCode.ER_XAER_RMFAIL.getCode()) {
                return true; // Maybe not prepared yet. Ignore such exceptions
            } else if (ex.getErrorCode() == ErrorCode.ER_XAER_NOTA.getCode()) {
                return true; // Transaction lost or recovered by others
            }
            TransactionLogger.error("XA ROLLBACK error", ex);
            return false;
        }
    }

    private static boolean tryCommitXA(Statement stmt, PreparedXATrans trans) {
        return tryCommit0(stmt, "XA COMMIT " + trans.toXid());
    }

    private static boolean tryCommitTSO(Statement stmt, PreparedXATrans trans, long commitTimestamp, long id,
                                        boolean supportAsyncCommit)
        throws SQLException {
        final XConnection xConnection;
        if (supportAsyncCommit) {
            try {
                stmt.execute(String.format(SET_DISTRIBUTED_TRX_ID, id));
                stmt.execute(SET_REMOVE_DISTRIBUTED_TRX);
            } catch (SQLException e) {
                logger.warn("Set async commit info failed.", e);
            }
        }
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
            logger.error("XA COMMIT error", ex);
            TransactionLogger.error("XA COMMIT error", ex);
            if (ex.getErrorCode() == ErrorCode.ER_XAER_RMFAIL.getCode()) {
                return true; // Maybe not prepared yet. Ignore such exceptions
            } else if (ex.getErrorCode() == ErrorCode.ER_XAER_NOTA.getCode()) {
                return false; // Transaction lost or recovered by others
            }
            return false;
        }
    }

    private String getRecoverXid(Long trxId, String bqual) {
        return String.format("'%s@%s', '%s', 2", RECOVER_GTRID_PREFIX, Long.toHexString(trxId), bqual);
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
