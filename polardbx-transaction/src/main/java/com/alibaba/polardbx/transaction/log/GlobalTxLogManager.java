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

package com.alibaba.polardbx.transaction.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.TableName;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.trx.TrxLogTableConstants;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.common.utils.LockUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.InstConfigAccessor;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.rpc.compatible.XPreparedStatement;
import com.alibaba.polardbx.transaction.trx.AsyncCommitTransaction;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.TransactionState;
import com.alibaba.polardbx.transaction.utils.XAUtils;
import com.google.protobuf.ByteString;
import com.alibaba.polardbx.transaction.jdbc.DeferredConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.common.constants.SystemTables.DRDS_GLOBAL_TX_LOG;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.ALTER_GLOBAL_TX_TABLE_ADD_MAX_PARTITION;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.ALTER_GLOBAL_TX_TABLE_ADD_PARTITION;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.ALTER_GLOBAL_TX_TABLE_COMMIT_TS;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.ALTER_GLOBAL_TX_TABLE_DROP_PARTITION_PREFIX;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.ALTER_GLOBAL_TX_TABLE_INIT_PARTITION;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.ALTER_GLOBAL_TX_TABLE_TYPE_ENUMS;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.ALTER_REDO_LOG_TABLE;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.APPEND_TRX;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.APPEND_TRX_DIGEST;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.APPEND_TRX_WITH_TS_DIGEST;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.CREATE_GLOBAL_TX_TABLE;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.CREATE_GLOBAL_TX_TABLE_V2;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.CREATE_REDO_LOG_TABLE;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.EXISTS_GLOBAL_TX_TABLE_V2;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.GLOBAL_TX_TABLE_GET_PARTITIONS;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.REDO_LOG_TABLE;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.SELECT_BY_ID;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.SELECT_BY_ID_DIGEST;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.SELECT_BY_ID_V2;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.SELECT_BY_ID_V2_ARCHIVE;
import static com.alibaba.polardbx.common.trx.TrxLogTableConstants.SET_DISTRIBUTED_TRX_ID;
import static com.alibaba.polardbx.common.utils.LockUtil.wrapWithInnodbLockWaitTimeout;
import static com.alibaba.polardbx.common.utils.LockUtil.wrapWithLockWaitTimeout;

public class GlobalTxLogManager extends AbstractLifecycle {

    protected final static Logger logger = LoggerFactory.getLogger(GlobalTxLogManager.class);

    private static final String GLOBAL_TX_LOG_TABLE = SystemTables.DRDS_GLOBAL_TX_LOG;

    private static volatile String currentServerAddr;

    private TransactionExecutor executor;

    private static final AtomicLong appendV2FailedCnt = new AtomicLong(0);
    private static final AtomicLong appendV2FailedLastTime = new AtomicLong(0);

    @Override
    public void doInit() {
        if (currentServerAddr == null) {
            synchronized (GlobalTxLogManager.class) {
                if (currentServerAddr == null) {
                    String ipAddress = System.getProperty("ipAddress");

                    if (TStringUtil.isEmpty(ipAddress)) {
                        logger.error("Cannot get ipAddress");
                    }
                    String managerPort = System.getProperty("managerPort");
                    if (TStringUtil.isEmpty(managerPort)) {
                        logger.error("Cannot get managerPort");
                    }
                    currentServerAddr = ipAddress + ":" + managerPort;
                }
            }
        }
    }

    public void setTransactionExecutor(TransactionExecutor executor) {
        this.executor = executor;
    }

    public TransactionExecutor getTransactionExecutor() {
        return this.executor;
    }

    /**
     * @return true if using trx log v2
     */
    public boolean appendTrxLog(long txid, TransactionType type, TransactionState state, ConnectionContext context,
                                IConnection conn) throws SQLException {
        if (0 == DynamicConfig.getInstance().getTrxLogMethod()) {
            append(txid, type, state, context, conn);
            return false;
        } else {
            // Use 1 to represent XA transaction.
            appendV2(txid, 1, conn);
            long lastLogTime = TransactionAttribute.LAST_LOG_TRX_LOG_V2.get();
            if (TransactionManager.shouldWriteEventLog(lastLogTime)
                && TransactionAttribute.LAST_LOG_TRX_LOG_V2.compareAndSet(lastLogTime, System.nanoTime())) {
                EventLogger.log(EventType.TRX_INFO, "Found use of A/B table as trx log");
            }
            return true;
        }
    }

    /**
     * @return true if using trx log v2
     */
    public boolean appendTrxLog(long txid, TransactionType type, TransactionState state, ConnectionContext context,
                                long commitTimestamp, IConnection conn) throws SQLException {
        if (0 == DynamicConfig.getInstance().getTrxLogMethod()) {
            append(txid, type, state, context, commitTimestamp, conn);
            return false;
        } else {
            appendV2(txid, commitTimestamp, conn);
            long lastLogTime = TransactionAttribute.LAST_LOG_TRX_LOG_V2.get();
            if (TransactionManager.shouldWriteEventLog(lastLogTime)
                && TransactionAttribute.LAST_LOG_TRX_LOG_V2.compareAndSet(lastLogTime, System.nanoTime())) {
                EventLogger.log(EventType.TRX_INFO, "Found use of A/B table as trx log");
            }
            return true;
        }
    }

    public static void append(long txid, TransactionType type, TransactionState state, ConnectionContext context,
                              IConnection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(APPEND_TRX)) {
            if (ps.isWrapperFor(XPreparedStatement.class)) {
                ps.unwrap(XPreparedStatement.class).setGalaxyDigest(APPEND_TRX_DIGEST);
            }
            ps.setObject(1, new TableName(DRDS_GLOBAL_TX_LOG));
            ps.setLong(2, txid);
            ps.setString(3, type.name());
            ps.setString(4, state.name());
            ps.setString(5, currentServerAddr);
            ps.setString(6, JSON.toJSONString(context));
            ps.executeUpdate();
        }
    }

    public static void append(long txid, TransactionType type, TransactionState state, ConnectionContext context,
                              long commitTimestamp, IConnection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(TrxLogTableConstants.APPEND_TRX_WITH_TS)) {
            if (ps.isWrapperFor(XPreparedStatement.class)) {
                ps.unwrap(XPreparedStatement.class).setGalaxyDigest(APPEND_TRX_WITH_TS_DIGEST);
            }
            ps.setObject(1, new TableName(DRDS_GLOBAL_TX_LOG));
            ps.setLong(2, txid);
            ps.setString(3, type.name());
            ps.setString(4, state.name());
            ps.setString(5, currentServerAddr);
            ps.setString(6, JSON.toJSONString(context));
            ps.setLong(7, commitTimestamp);
            ps.executeUpdate();
        }
    }

    public static void appendWithLockWaitTimeout(long txid, TransactionType type, TransactionState state,
                                                 ConnectionContext context,
                                                 IConnection conn) throws SQLException {
        AtomicReference<SQLException> exception = new AtomicReference<>();
        wrapWithLockWaitTimeout(conn, 3, () -> {
            try {
                wrapWithInnodbLockWaitTimeout(conn, 3, () -> {
                    try {
                        append(txid, type, state, context, conn);
                    } catch (SQLException e) {
                        logger.error(e);
                        exception.set(e);
                    }
                });
            } catch (SQLException e) {
                logger.error(e);
                exception.set(e);
            }
        });
        if (null != exception.get()) {
            throw exception.get();
        }
    }

    public static void appendV2(long txid, long commitTimestamp, IConnection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(TrxLogTableConstants.APPEND_TRX_V2)) {
            ps.setLong(1, txid);
            ps.setLong(2, commitTimestamp);
            ps.executeUpdate();
        } catch (SQLException e) {
            EventLogger.log(EventType.TRX_LOG_ERR, "Append trx log v2 failed, caused by " + e.getMessage());
            try {
                handleV2LogError();
            } catch (Throwable t) {
                EventLogger.log(EventType.TRX_LOG_ERR, "Handle trx log v2 error failed, "
                    + "caused by " + e.getMessage());
                logger.error("Handle trx log v2 error failed", t);
            }
            throw e;
        }
    }

    private static void handleV2LogError() throws SQLException {
        long lastErrTime = appendV2FailedLastTime.get();
        if ((System.nanoTime() - lastErrTime) / 1000000000 > 600
            && appendV2FailedLastTime.compareAndSet(lastErrTime, System.nanoTime())) {
            // First error in 10 min, reset err cnt.
            appendV2FailedCnt.set(0);
        }
        // 10 err occurs in the last 10 min, switch to legacy method for safety.
        if (appendV2FailedCnt.incrementAndGet() == 10) {
            try {
                if (0 == DynamicConfig.getInstance().getTrxLogMethod()) {
                    return;
                }

                turnOffNewTrxLogMethod();
            } finally {
                appendV2FailedCnt.set(0);
            }
        }
    }

    public static void turnOffNewTrxLogMethod() throws SQLException {
        // Set global.
        Properties properties = new Properties();
        properties.setProperty(ConnectionProperties.TRX_LOG_METHOD, String.valueOf(0));
        MetaDbUtil.setGlobal(properties);
    }

    public static void appendV2WithLockWaitTimeout(long txid, long commitTimestamp, IConnection conn)
        throws SQLException {
        AtomicReference<SQLException> exception = new AtomicReference<>();
        wrapWithLockWaitTimeout(conn, 3, () -> {
            try {
                wrapWithInnodbLockWaitTimeout(conn, 3, () -> {
                    try {
                        appendV2(txid, commitTimestamp, conn);
                    } catch (SQLException e) {
                        logger.error(e);
                        exception.set(e);
                    }
                });
            } catch (SQLException e) {
                logger.error(e);
                EventLogger.log(EventType.TRX_LOG_ERR, "Append trx log v2 with lock wait timeout "
                    + "failed, caused by " + e.getMessage());
                exception.set(e);
            }
        });
        if (null != exception.get()) {
            throw exception.get();
        }
    }

    public GlobalTxLog get(String primaryGroup, XAUtils.XATransInfo transInfo, String schema) throws SQLException {
        IDataSource dataSource = executor.getGroupExecutor(primaryGroup).getDataSource();
        try (IConnection conn = dataSource.getConnection()) {
            if (TransactionManager.getInstance(schema).support2pcOpt()) {
                try (Statement stmt = conn.createStatement()) {
                    ResultSet rs = stmt.executeQuery(String.format("call dbms_xa.find_by_xid('%s', '%s', 1)",
                        transInfo.gtrid, XAUtils.toBqualString(primaryGroup, 0L)));
                    if (rs.next()) {
                        String status = rs.getString("Status");
                        String tso = rs.getString("GCN");
                        String csr = rs.getString("CSR");

                        TransactionLogger.warn("Found 2pc opt trx log, status " + status + ", tso " + tso + ", csr " + csr
                            + ", gtrid " + transInfo.gtrid + ", bqual " + transInfo.trimedBqual);

                        if ("ATTACHED".equalsIgnoreCase(status)) {
                            GlobalTxLog trans = new GlobalTxLog();
                            trans.setGroup(primaryGroup);
                            trans.setTxid(transInfo.transId);
                            trans.setState(TransactionState.ATTACHED);
                            return trans;
                        } else if ("ROLLBACK".equalsIgnoreCase(status)) {
                            GlobalTxLog trans = new GlobalTxLog();
                            trans.setGroup(primaryGroup);
                            trans.setTxid(transInfo.transId);
                            trans.setState(TransactionState.ABORTED);
                            return trans;
                        } else if ("COMMIT".equalsIgnoreCase(status)) {
                            GlobalTxLog trans = new GlobalTxLog();
                            trans.setGroup(primaryGroup);
                            trans.setTxid(transInfo.transId);
                            if ("ASSIGNED_GCN".equalsIgnoreCase(csr)) {
                                trans.setType(TransactionType.TSO);
                                if (!"18446744073709551615".equals(tso)) {
                                    trans.setCommitTimestamp(Long.valueOf(tso));
                                }
                            } else {
                                trans.setType(TransactionType.XA);
                            }
                            trans.setState(TransactionState.SUCCEED);
                            return trans;
                        }
                    }

                    rs = stmt.executeQuery(String.format("call dbms_xa.find_by_xid('%s', '%s', 1)",
                        transInfo.gtrid, XAUtils.toBqualString(primaryGroup)));
                    if (rs.next()) {
                        String status = rs.getString("Status");
                        String tso = rs.getString("GCN");
                        String csr = rs.getString("CSR");

                        TransactionLogger.warn("Found 2pc opt trx log, status " + status + ", tso " + tso + ", csr " + csr
                            + ", gtrid " + transInfo.gtrid + ", bqual " + transInfo.trimedBqual);

                        if ("ATTACHED".equalsIgnoreCase(status)) {
                            GlobalTxLog trans = new GlobalTxLog();
                            trans.setGroup(primaryGroup);
                            trans.setTxid(transInfo.transId);
                            trans.setState(TransactionState.ATTACHED);
                            return trans;
                        } else if ("ROLLBACK".equalsIgnoreCase(status)) {
                            GlobalTxLog trans = new GlobalTxLog();
                            trans.setGroup(primaryGroup);
                            trans.setTxid(transInfo.transId);
                            trans.setState(TransactionState.ABORTED);
                            return trans;
                        } else if ("COMMIT".equalsIgnoreCase(status)) {
                            GlobalTxLog trans = new GlobalTxLog();
                            trans.setGroup(primaryGroup);
                            trans.setTxid(transInfo.transId);
                            if ("ASSIGNED_GCN".equalsIgnoreCase(csr)) {
                                trans.setType(TransactionType.TSO);
                                if (!"18446744073709551615".equals(tso)) {
                                    trans.setCommitTimestamp(Long.valueOf(tso));
                                }
                            } else {
                                trans.setType(TransactionType.XA);
                            }
                            trans.setState(TransactionState.SUCCEED);
                            return trans;
                        }
                    }
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(SELECT_BY_ID)) {
                if (ps.isWrapperFor(XPreparedStatement.class)) {
                    ps.unwrap(XPreparedStatement.class).setGalaxyDigest(SELECT_BY_ID_DIGEST);
                }
                ps.setObject(1, new TableName(GLOBAL_TX_LOG_TABLE));
                ps.setLong(2, transInfo.transId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        GlobalTxLog trans = new GlobalTxLog();
                        trans.setGroup(primaryGroup);
                        trans.setTxid(transInfo.transId);
                        trans.setType(TransactionType.valueOf(rs.getString(1)));
                        trans.setState(TransactionState.valueOf(rs.getString(2)));
                        trans.setServerAddr(rs.getString(3));
                        trans.setContext(JSON.parseObject(rs.getString(4), ConnectionContext.class));
                        long commitTimestamp = rs.getLong(5);
                        if (commitTimestamp > 0) { // zero for NULL
                            trans.setCommitTimestamp(commitTimestamp);
                        }
                        return trans;
                    }
                }
            }
        }
        return null;
    }

    public GlobalTxLog get(String primaryGroup, long txid) throws SQLException {
        AtomicReference<SQLException> exception = new AtomicReference<>();
        AtomicReference<GlobalTxLog> trans = new AtomicReference<>();
        IDataSource dataSource = executor.getGroupExecutor(primaryGroup).getDataSource();
        try (IConnection conn = dataSource.getConnection();
            PreparedStatement ps = conn.prepareStatement(SELECT_BY_ID)) {
            wrapWithLockWaitTimeout(conn, 3, () -> {
                try {
                    if (ps.isWrapperFor(XPreparedStatement.class)) {
                        ps.unwrap(XPreparedStatement.class).setGalaxyDigest(SELECT_BY_ID_DIGEST);
                    }
                    ps.setObject(1, new TableName(DRDS_GLOBAL_TX_LOG));
                    ps.setLong(2, txid);
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            trans.set(new GlobalTxLog());
                            trans.get().setGroup(primaryGroup);
                            trans.get().setTxid(txid);
                            trans.get().setType(TransactionType.valueOf(rs.getString(1)));
                            trans.get().setState(TransactionState.valueOf(rs.getString(2)));
                            trans.get().setServerAddr(rs.getString(3));
                            trans.get().setContext(JSON.parseObject(rs.getString(4), ConnectionContext.class));
                            long commitTimestamp = rs.getLong(5);
                            if (commitTimestamp > 0) { // zero for NULL
                                trans.get().setCommitTimestamp(commitTimestamp);
                            }
                        }
                    }
                } catch (SQLException e) {
                    exception.set(e);
                }
            });
        }

        if (null != exception.get()) {
            throw exception.get();
        }

        return trans.get();
    }

    /**
     * Get trx log V2, either an async commit log, or a new trx log.
     */
    public static GlobalTxLog getV2(IDataSource dataSource, long txid, boolean dn57) throws SQLException {
        AtomicReference<SQLException> exception = new AtomicReference<>();
        AtomicReference<GlobalTxLog> trans = new AtomicReference<>();
        try (DeferredConnection conn = new DeferredConnection(dataSource.getConnection(),
            InstConfUtil.getBool(ConnectionParams.USING_RDS_RESULT_SKIP))) {
            wrapWithLockWaitTimeout(conn, 3, () -> {
                try (Statement stmt = conn.createStatement()) {
                    conn.executeLater("begin");
                    if (dn57) {
                        conn.executeLater(String.format(SET_DISTRIBUTED_TRX_ID, txid));
                        conn.executeLater(TrxLogTableConstants.RECOVER_TIMESTAMP_SQL);
                    }
                    ResultSet rs = stmt.executeQuery(String.format(SELECT_BY_ID_V2, txid));
                    if (!rs.next()) {
                        // Not found in table A, try to find in table B.
                        try {
                            rs = stmt.executeQuery(String.format(SELECT_BY_ID_V2_ARCHIVE, txid));
                        } catch (SQLException e) {
                            if (e.getMessage().contains("doesn't exist")) {
                                // Ignore. Archive table is already dropped.
                                return;
                            } else {
                                throw e;
                            }
                        }
                        if (!rs.next()) {
                            // Not found in table B either.
                            return;
                        }
                    }
                    // Already call rs.next().
                    trans.set(new GlobalTxLog());
                    trans.get().setTxid(txid);
                    trans.get().setCommitTimestamp(rs.getLong(1));
                    trans.get().setParticipants(rs.getInt(2));
                    trans.get().setType(TransactionType.TSO);
                    if (0 == trans.get().getCommitTimestamp()) {
                        // Aborted transaction.
                        trans.get().setState(TransactionState.ABORTED);
                    } else if (1 == trans.get().getCommitTimestamp()) {
                        // Committed XA transaction.
                        trans.get().setType(TransactionType.XA);
                        trans.get().setState(TransactionState.SUCCEED);
                    } else if (AsyncCommitTransaction.isMinCommitSeq(trans.get().getCommitTimestamp())) {
                        // Prepared/Committed async commit transaction, just mark it as PREPARE.
                        trans.get().setState(TransactionState.PREPARE);
                    } else {
                        // Committed normal TSO transaction.
                        trans.get().setState(TransactionState.SUCCEED);
                    }
                } catch (SQLException e) {
                    exception.set(e);
                } finally {
                    // Cleanup async commit variables.
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("rollback");
                    } catch (SQLException e) {
                        logger.warn("rollback failed when get v2 log", e);
                    }
                }
            });
        }

        // Re-throw error.
        if (null != exception.get()) {
            throw exception.get();
        }
        return trans.get();
    }

    public static int rotate(IDataSource dataSource, long beforeTxid, long nextTxid) {
        try (IConnection connection = dataSource.getConnection()) {
            AtomicLong dropped = new AtomicLong();
            LockUtil.wrapWithLockWaitTimeout(connection, 10, () -> {
                try (Statement stmt = connection.createStatement()) {
                    ArrayList<String> partitionsWillDrop = new ArrayList<>();
                    long txidUpperBound = Long.MIN_VALUE;
                    try (ResultSet rs = stmt.executeQuery(GLOBAL_TX_TABLE_GET_PARTITIONS)) {
                        while (rs.next()) {
                            final String partitionName = rs.getString(1);
                            if (partitionName == null) {
                                throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG,
                                    "Rotate global tx log on non-partitioned table");
                            }
                            final String partitionDescText = rs.getString(2);
                            if ("MAXVALUE".equalsIgnoreCase(partitionDescText)) {
                                continue;
                            }
                            try {
                                long maxTxidInPartition = Long.parseLong(partitionDescText);
                                if (maxTxidInPartition < beforeTxid) {
                                    final long tableRows = rs.getLong(3);
                                    dropped.addAndGet(tableRows);
                                    partitionsWillDrop.add("`" + partitionName + "`");
                                }
                                txidUpperBound = Math.max(txidUpperBound, maxTxidInPartition);
                            } catch (NumberFormatException e) {
                                throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG,
                                    "Invalid partition description for partition " + partitionName);
                            }
                        }
                    }
                    if (nextTxid > txidUpperBound) {
                        logger.info("Creating new partition" + "p_" + nextTxid + " on global tx log");
                        stmt.executeUpdate(
                            String.format(ALTER_GLOBAL_TX_TABLE_ADD_PARTITION, "p_" + nextTxid, nextTxid));
                    }
                    if (!partitionsWillDrop.isEmpty()) {
                        String dropSql =
                            ALTER_GLOBAL_TX_TABLE_DROP_PARTITION_PREFIX + String.join(",", partitionsWillDrop);
                        logger.info("Purging global tx log with ddl " + dropSql.replace("\n", " "));
                        stmt.executeUpdate(dropSql);
                    }
                } catch (SQLException e) {
                    TransactionLogger.error("Rotate global transaction log with " + beforeTxid + " failed", e);
                }
            });
            return dropped.intValue();
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG, e,
                "Rotate global transaction log with " + beforeTxid + " failed");
        }
    }

    public static void createTables(IDataSource dataSource, long initTxid, Set<String> dnSet) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement()) {
            String instanceId = ((TGroupDataSource) dataSource).getMasterSourceAddress();
            if (dnSet.add(instanceId)) {
                // One table only for each DN.
                createGlobalTxLogTableV2(stmt);
            }
            createGlobalTxLogTable(stmt, initTxid);
            createRedoLogTable(stmt);
        } catch (SQLException ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG, ex,
                "Failed to create transaction log tables: " + ex.getMessage());
        }
    }

    /**
     * Create DRDS_GLOBAL_TX_LOG table or alter the `TYPE` column of it
     */
    private static void createGlobalTxLogTable(Statement stmt, long initTxid) throws SQLException {
        boolean needCreate = false;
        boolean needAlterType = true;
        boolean needAddCommitTs = true;
        try (ResultSet rs = stmt.executeQuery("SHOW COLUMNS FROM " + DRDS_GLOBAL_TX_LOG)) {
            while (rs.next()) {
                String field = rs.getString(1);
                String type = rs.getString(2);
                // enum('TCC','XA') -> enum('TCC','XA','BED','TSO','HLC')
                if ("TYPE".equalsIgnoreCase(field) && TStringUtil.containsIgnoreCase(type, "TSO")) {
                    needAlterType = false;
                }
                if ("COMMIT_TS".equals(field)) {
                    needAddCommitTs = false;
                }
            }
        } catch (SQLException ex) {
            if (ex.getErrorCode() == ErrorCode.ER_NO_SUCH_TABLE.getCode()) {
                needCreate = true;
            } else {
                throw ex;
            }
        }

        boolean needAlterPartition = false;
        boolean needAlterMaxPartition = true;
        if (!needCreate) {
            try (ResultSet rs = stmt.executeQuery(GLOBAL_TX_TABLE_GET_PARTITIONS)) {
                if (rs.next()) {
                    String partitionName = rs.getString(1);
                    String partitionDesc = rs.getString(2);
                    // Only one row with NULL partitionName means this is not a partition table.
                    if (partitionName == null) {
                        needAlterPartition = true;
                        if (rs.next()) {
                            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG,
                                DRDS_GLOBAL_TX_LOG + "have multiple partitions with NULL partition name");
                        }
                    }
                    if ("MAXVALUE".equalsIgnoreCase(partitionDesc)) {
                        needAlterMaxPartition = false;
                    }
                    while (rs.next()) {
                        partitionDesc = rs.getString(2);
                        if ("MAXVALUE".equalsIgnoreCase(partitionDesc)) {
                            needAlterMaxPartition = false;
                        }
                    }
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG,
                        "Empty result in " + DRDS_GLOBAL_TX_LOG + " partitions");
                }
            }
        }

        if (needCreate) {
            logger.warn("Creating system table: GLOBAL_TX_LOG");
            stmt.executeUpdate(String.format(CREATE_GLOBAL_TX_TABLE, "p_" + initTxid, initTxid));
            return;
        }
        if (needAlterType) {
            logger.warn("Upgrading system table: GLOBAL_TX_LOG (Alter TYPE enums)");
            stmt.executeUpdate(ALTER_GLOBAL_TX_TABLE_TYPE_ENUMS);
        }
        if (needAddCommitTs) {
            logger.warn("Upgrading system table: GLOBAL_TX_LOG (Add column COMMIT_TS)");
            try {
                stmt.executeUpdate(ALTER_GLOBAL_TX_TABLE_COMMIT_TS);
            } catch (SQLException ex) {
                // ignore 'Duplicate column'
                if (!"42S21".equals(ex.getSQLState())) {
                    throw ex;
                }
            }
        }
        if (needAlterPartition) {
            logger.warn("Upgrading system table: GLOBAL_TX_LOG (Create partition)");
            stmt.executeUpdate(ALTER_GLOBAL_TX_TABLE_INIT_PARTITION);
            stmt.executeUpdate(
                String.format(ALTER_GLOBAL_TX_TABLE_ADD_PARTITION, "p_" + initTxid, initTxid));
        } else if (needAlterMaxPartition) {
            logger.warn("Upgrading system table: GLOBAL_TX_LOG (Create MAXVALUE partition)");
            stmt.executeUpdate(ALTER_GLOBAL_TX_TABLE_ADD_MAX_PARTITION);
        }
    }

    private static void createGlobalTxLogTableV2(Statement stmt) throws SQLException {
        try {
            ResultSet rs = stmt.executeQuery(EXISTS_GLOBAL_TX_TABLE_V2);
            if (rs.next()) {
                // Table already exists.
                return;
            }
        } catch (Throwable t) {
            logger.warn("Test global tx log table v2 exists failed", t);
        }
        logger.warn("Creating system table: mysql.GLOBAL_TX_LOG_V2");
        stmt.executeUpdate(CREATE_GLOBAL_TX_TABLE_V2);
    }

    /**
     * Create DRDS_REDO_LOG table or add a `SCHEMA` column
     */
    private static void createRedoLogTable(Statement stmt) throws SQLException {
        boolean needCreate = false;
        boolean needUpgrade = true;
        try (ResultSet rs = stmt.executeQuery("SHOW COLUMNS FROM " + REDO_LOG_TABLE)) {
            while (rs.next()) {
                String field = rs.getString(1);
                if ("SCHEMA".equalsIgnoreCase(field)) {
                    needUpgrade = false;
                }
            }
        } catch (SQLException ex) {
            if (ex.getErrorCode() == ErrorCode.ER_NO_SUCH_TABLE.getCode()) {
                needCreate = true;
            } else {
                throw ex;
            }
        }

        if (needCreate) {
            logger.warn("Creating system table: REDO_LOG");
            stmt.executeUpdate(CREATE_REDO_LOG_TABLE);
            return;
        }
        if (needUpgrade) {
            logger.warn("Upgrading system table: REDO_LOG");
            try {
                stmt.executeUpdate(ALTER_REDO_LOG_TABLE);
            } catch (SQLException ex) {
                // ignore 'Duplicate column'
                if (!"42S21".equals(ex.getSQLState())) {
                    throw ex;
                }
            }
        }
    }

    public IDataSource getDatasource(String group) {
        return executor.getGroupExecutor(group).getDataSource();
    }
}
