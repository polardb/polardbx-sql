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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.TableName;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.common.utils.LockUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.rpc.compatible.XPreparedStatement;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.TransactionState;
import com.alibaba.polardbx.transaction.TsoTransaction;
import com.alibaba.polardbx.transaction.utils.XAUtils;
import com.google.protobuf.ByteString;

import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.transaction.TsoTransaction.SET_DISTRIBUTED_TRX_ID;

public class GlobalTxLogManager extends AbstractLifecycle {

    protected final static Logger logger = LoggerFactory.getLogger(GlobalTxLogManager.class);

    private static final String GLOBAL_TX_LOG_TABLE = SystemTables.DRDS_GLOBAL_TX_LOG;
    private static final String GLOBAL_ASYNC_COMMIT_TX_LOG_TABLE = SystemTables.POLARDBX_ASYNC_COMMIT_TX_LOG_TABLE;
    private static final String GLOBAL_ASYNC_COMMIT_TX_LOG_DATABASE = "mysql";
    private static final String REDO_LOG_TABLE = SystemTables.DRDS_REDO_LOG;

    private static final String CREATE_REDO_LOG_TABLE =
        "CREATE TABLE IF NOT EXISTS `" + REDO_LOG_TABLE + "` (\n"
            + "  `TXID` BIGINT NOT NULL,\n"
            + "  `SCHEMA` VARCHAR(64) NULL,\n"
            + "  `SEQ`  INT(11) NOT NULL,\n"
            + "  `INFO` LONGTEXT NOT NULL,\n"
            + "  PRIMARY KEY (`TXID`, `SEQ`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n";

    private static final String GLOBAL_TX_TABLE_MAX_PARTITION = "p_unlimited";

    private static final String CREATE_GLOBAL_TX_TABLE =
        "CREATE TABLE IF NOT EXISTS `" + GLOBAL_TX_LOG_TABLE + "` (\n"
            + "  `TXID` BIGINT NOT NULL,\n"
            + "  `START_TIME` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            + "  `TYPE` ENUM('TCC', 'XA', 'BED', 'TSO', 'HLC') NOT NULL,\n"
            + "  `STATE` ENUM('PREPARE', 'COMMIT', 'ROLLBACK', 'SUCCEED', 'ABORTED') NOT NULL,\n"
            + "  `RETRIES` INT(11) NOT NULL DEFAULT 0,\n"
            + "  `COMMIT_TS` BIGINT DEFAULT NULL,\n"
            + "  `PARTICIPANTS` BLOB DEFAULT NULL,\n"
            + "  `TIMEOUT` TIMESTAMP NULL,\n"
            + "  `SERVER_ADDR` VARCHAR(21) NOT NULL,\n"
            + "  `CONTEXT` TEXT NOT NULL,\n"
            + "  `ERROR` TEXT NULL,\n"
            + "  PRIMARY KEY (`TXID`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8\n"
            + "PARTITION BY RANGE (`TXID`) (PARTITION `%s` VALUES LESS THAN (%d), PARTITION `"
            + GLOBAL_TX_TABLE_MAX_PARTITION + "` VALUES LESS THAN MAXVALUE)";

    /**
     * Support Async Commit.
     * Column: TXID, COMMIT_TS, N_PARTICIPANTS
     */
    private static final String CREATE_GLOBAL_TX_TABLE_V2 =
        "CREATE TABLE IF NOT EXISTS `" + GLOBAL_ASYNC_COMMIT_TX_LOG_DATABASE + "`.`" + GLOBAL_ASYNC_COMMIT_TX_LOG_TABLE
            + "` (\n"
            + "  `TXID` BIGINT UNSIGNED NOT NULL,\n"
            + "  `TRX_SEQ` BIGINT UNSIGNED NOT NULL DEFAULT 18446744073709551615 COMMENT \"DEFAULT INVALID_SEQUENCE_NUMBER\",\n"
            + "  `N_PARTICIPANTS` INT UNSIGNED NOT NULL DEFAULT 0,\n"
            + "  PRIMARY KEY (`TXID`)\n"
            + ") ENGINE=InnoDB DEFAULT CHARSET=utf8\n"
            + "PARTITION BY RANGE (`TXID`) (PARTITION `%s` VALUES LESS THAN (%d), PARTITION `"
            + GLOBAL_TX_TABLE_MAX_PARTITION + "` VALUES LESS THAN MAXVALUE)";

    private static final String EXISTS_GLOBAL_TX_TABLE_V2 =
        "SELECT 1 FROM information_schema.tables WHERE table_schema = '"
            + GLOBAL_ASYNC_COMMIT_TX_LOG_DATABASE + "' AND table_name = '" + GLOBAL_ASYNC_COMMIT_TX_LOG_TABLE + "'";

    private static final String ALTER_GLOBAL_TX_TABLE_INIT_PARTITION =
        "ALTER TABLE `" + GLOBAL_TX_LOG_TABLE + "` \n"
            + "PARTITION BY RANGE (`TXID`) (PARTITION `" + GLOBAL_TX_TABLE_MAX_PARTITION
            + "` VALUES LESS THAN MAXVALUE)";

    private static final String ALTER_GLOBAL_TX_TABLE_ADD_MAX_PARTITION =
        "ALTER TABLE `" + GLOBAL_TX_LOG_TABLE + "` \n"
            + "PARTITION BY RANGE (`TXID`) (PARTITION `" + GLOBAL_TX_TABLE_MAX_PARTITION
            + "` VALUES LESS THAN MAXVALUE)";

    private static final String ALTER_GLOBAL_TX_TABLE_ADD_PARTITION =
        "ALTER TABLE `" + GLOBAL_TX_LOG_TABLE + "` \n"
            + "REORGANIZE PARTITION `" + GLOBAL_TX_TABLE_MAX_PARTITION + "` INTO \n"
            + "(PARTITION `%s` VALUES LESS THAN (%d), PARTITION `" + GLOBAL_TX_TABLE_MAX_PARTITION
            + "` VALUES LESS THAN MAXVALUE)";

    private static final String ALTER_GLOBAL_TX_TABLE_DROP_PARTITION_PREFIX =
        "ALTER TABLE `" + GLOBAL_TX_LOG_TABLE + "` \n"
            + "DROP PARTITION ";

    private static final String GLOBAL_TX_TABLE_GET_PARTITIONS =
        "SELECT `PARTITION_NAME`, `PARTITION_DESCRIPTION`, `TABLE_ROWS` FROM  INFORMATION_SCHEMA.PARTITIONS\n"
            + "WHERE TABLE_NAME = '" + GLOBAL_TX_LOG_TABLE + "'\n"
            + "AND TABLE_SCHEMA = DATABASE()";

    private static final String ALTER_GLOBAL_TX_TABLE_TYPE_ENUMS =
        "ALTER TABLE `" + GLOBAL_TX_LOG_TABLE + "` MODIFY COLUMN `TYPE` "
            + "enum('TCC', 'XA', 'BED', 'TSO', 'HLC') NOT NULL";

    private static final String ALTER_GLOBAL_TX_TABLE_COMMIT_TS =
        "ALTER TABLE `" + GLOBAL_TX_LOG_TABLE + "` "
            + "ADD COLUMN `COMMIT_TS` BIGINT DEFAULT NULL, "
            + "ADD COLUMN `PARTICIPANTS` BLOB DEFAULT NULL, "
            + "ALGORITHM=INPLACE, LOCK=NONE";

    /**
     * Note: We want to use same digest for all sql on different physical DB, so we treat table as a parameter.
     */

    private static final String APPEND_TRX =
        "INSERT INTO ? (`TXID`, `TYPE`, `STATE`, `SERVER_ADDR`, `CONTEXT`) VALUES (?, ?, ?, ?, ?)";

    private static ByteString APPEND_TRX_DIGEST;

    private static final String APPEND_TRX_WITH_TS =
        "INSERT INTO ? (`TXID`, `TYPE`, `STATE`, `SERVER_ADDR`, `CONTEXT`, `COMMIT_TS`) VALUES (?, ?, ?, ?, ?, ?)";

    /**
     * Column: TXID, START_TIME, TYPE, STATE, COMMIT_TS, N_PARTICIPANTS, SERVER_ADDR, EXTRA
     */
    private static final String APPEND_ASYNC_COMMIT_TRX =
        "INSERT INTO " + GLOBAL_ASYNC_COMMIT_TX_LOG_DATABASE + "." + GLOBAL_ASYNC_COMMIT_TX_LOG_TABLE +
            " (`TXID`, `TRX_SEQ`) VALUES (%s, %s)";

    private static final String DELETE_ASYNC_COMMIT_TRX =
        "DELETE FROM " + GLOBAL_ASYNC_COMMIT_TX_LOG_DATABASE + "." + GLOBAL_ASYNC_COMMIT_TX_LOG_TABLE +
            " WHERE `TXID` = ?";

    private static ByteString APPEND_TRX_WITH_TS_DIGEST;

    private static final String SELECT_BY_ID =
        "SELECT `TYPE`, `STATE`, `SERVER_ADDR`, `CONTEXT`, `COMMIT_TS` FROM ? WHERE `TXID` = ?";

    private static ByteString SELECT_BY_ID_DIGEST;

    private static final String ALTER_REDO_LOG_TABLE = "ALTER TABLE `" + REDO_LOG_TABLE + "` "
        + "ADD COLUMN `SCHEMA` VARCHAR(64) NULL AFTER `TXID`";

    /**
     * Column: TXID, START_TIME, TYPE, STATE, COMMIT_TS, N_PARTICIPANTS, SERVER_ADDR, EXTRA
     */
    private static final String SELECT_BY_ID_V2 =
        "SELECT `TRX_SEQ`, `N_PARTICIPANTS` FROM %s WHERE `TXID` = %s";

    /**
     * 18446744073709551611 is a magic snapshot sequence. Using it you can see prepared trx.
     */
    private static final String RECOVER_TIMESTAMP_SQL =
        "SET innodb_snapshot_seq = 18446744073709551611";

    private String currentServerAddr;

    private TransactionExecutor executor;

    static {
        try {
            final MessageDigest md5 = MessageDigest.getInstance("md5");
            APPEND_TRX_DIGEST = ByteString.copyFrom(md5.digest(APPEND_TRX.getBytes()));
            APPEND_TRX_WITH_TS_DIGEST = ByteString.copyFrom(md5.digest(APPEND_TRX_WITH_TS.getBytes()));
            SELECT_BY_ID_DIGEST = ByteString.copyFrom(md5.digest(SELECT_BY_ID.getBytes()));
        } catch (Exception e) {
            logger.error(e);
        }
    }

    @Override
    public void doInit() {
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

    public void setTransactionExecutor(TransactionExecutor executor) {
        this.executor = executor;
    }

    public TransactionExecutor getTransactionExecutor() {
        return this.executor;
    }

    public void append(long txid, TransactionType type, TransactionState state, ConnectionContext context,
                       IConnection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(APPEND_TRX)) {
            if (ps.isWrapperFor(XPreparedStatement.class)) {
                ps.unwrap(XPreparedStatement.class).setGalaxyDigest(APPEND_TRX_DIGEST);
            }
            ps.setObject(1, new TableName(GLOBAL_TX_LOG_TABLE));
            ps.setLong(2, txid);
            ps.setString(3, type.name());
            ps.setString(4, state.name());
            ps.setString(5, currentServerAddr);
            ps.setString(6, JSON.toJSONString(context));
            ps.executeUpdate();
        }
    }

    public void append(long txid, TransactionType type, TransactionState state, ConnectionContext context,
                       long commitTimestamp, IConnection conn) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(APPEND_TRX_WITH_TS)) {
            if (ps.isWrapperFor(XPreparedStatement.class)) {
                ps.unwrap(XPreparedStatement.class).setGalaxyDigest(APPEND_TRX_WITH_TS_DIGEST);
            }
            ps.setObject(1, new TableName(GLOBAL_TX_LOG_TABLE));
            ps.setLong(2, txid);
            ps.setString(3, type.name());
            ps.setString(4, state.name());
            ps.setString(5, currentServerAddr);
            ps.setString(6, JSON.toJSONString(context));
            ps.setLong(7, commitTimestamp);
            ps.executeUpdate();
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
        IDataSource dataSource = executor.getGroupExecutor(primaryGroup).getDataSource();
        try (IConnection conn = dataSource.getConnection();
            PreparedStatement ps = conn.prepareStatement(SELECT_BY_ID)) {
            if (ps.isWrapperFor(XPreparedStatement.class)) {
                ps.unwrap(XPreparedStatement.class).setGalaxyDigest(SELECT_BY_ID_DIGEST);
            }
            ps.setObject(1, new TableName(GLOBAL_TX_LOG_TABLE));
            ps.setLong(2, txid);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    GlobalTxLog trans = new GlobalTxLog();
                    trans.setGroup(primaryGroup);
                    trans.setTxid(txid);
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
        return null;
    }

    public static GlobalTxLog getAsyncCommitLog(IDataSource dataSource, long txid, String schema) throws SQLException {
        if (!TransactionManager.getInstance(schema).supportAsyncCommit()) {
            return null;
        }
        final AtomicReference<SQLException> exception = new AtomicReference<>();
        final AtomicReference<GlobalTxLog> transRef = new AtomicReference<>();
        try (IConnection conn = dataSource.getConnection()) {
            LockUtil.wrapWithLockWaitTimeout(conn, 5, stmt -> {
                try {
                    stmt.execute("begin");
                    try {
                        stmt.execute(String.format(SET_DISTRIBUTED_TRX_ID, txid));
                        stmt.execute(RECOVER_TIMESTAMP_SQL);
                        ResultSet rs = stmt.executeQuery(
                            String.format(SELECT_BY_ID_V2,
                                GLOBAL_ASYNC_COMMIT_TX_LOG_DATABASE + "." + GLOBAL_ASYNC_COMMIT_TX_LOG_TABLE,
                                txid));
                        if (rs.next()) {
                            GlobalTxLog trans = new GlobalTxLog();
                            trans.setTxid(txid);
                            trans.setCommitTimestamp(rs.getLong(1));
                            trans.setParticipants(rs.getInt(2));
                            if (0 == trans.getCommitTimestamp()) {
                                trans.setState(TransactionState.ABORTED);
                                transRef.set(trans);
                            } else if (TsoTransaction.isMinCommitSeq(trans.getCommitTimestamp())) {
                                trans.setState(TransactionState.PREPARE);
                                transRef.set(trans);
                            } else {
                                logger.warn("Found invalid trans format.");
                            }
                        }
                    } finally {
                        stmt.execute("rollback");
                    }
                } catch (SQLException e) {
                    exception.set(e);
                }
            });
        }

        // Re-throw error.
        if (null != exception.get()) {
            throw exception.get();
        }
        return transRef.get();
    }

    public static void appendAsyncCommitLog(long txid, long commitTimestamp, IConnection conn) throws SQLException {
        AtomicReference<SQLException> exception = new AtomicReference<>();
        LockUtil.wrapWithLockWaitTimeout(conn, 5, stmt -> {
            try {
                stmt.execute(String.format(APPEND_ASYNC_COMMIT_TRX, txid, commitTimestamp));
            } catch (SQLException e) {
                logger.warn("Append async commit log failed.", e);
                exception.set(e);
            }
        });

        if (null != exception.get()) {
            throw exception.get();
        }
    }

    public static void deleteAsyncCommitLog(long txid, IConnection conn) throws SQLException {
        AtomicReference<SQLException> exception = new AtomicReference<>();
        LockUtil.wrapWithLockWaitTimeout(conn, 5, stmt -> {
            try {
                stmt.execute(String.format(DELETE_ASYNC_COMMIT_TRX, txid));
            } catch (SQLException e) {
                logger.warn("Delete async commit log failed.", e);
                exception.set(e);
            }
        });

        if (null != exception.get()) {
            throw exception.get();
        }
    }

    public static int rotate(IDataSource dataSource, long beforeTxid, long nextTxid) {
        try (IConnection connection = dataSource.getConnection()) {
            AtomicLong dropped = new AtomicLong();
            LockUtil.wrapWithLockWaitTimeout(connection, 60, stmt -> {
                try {
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

    public static void createTables(IDataSource dataSource, long initTxid) {
        try (Connection conn = dataSource.getConnection();
            Statement stmt = conn.createStatement()) {
            createGlobalTxLogTable(stmt, initTxid);
            createGlobalTxLogTableV2(stmt, initTxid);
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
        try (ResultSet rs = stmt.executeQuery("SHOW COLUMNS FROM " + GLOBAL_TX_LOG_TABLE)) {
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
                                GLOBAL_TX_LOG_TABLE + "have multiple partitions with NULL partition name");
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
                        "Empty result in " + GLOBAL_TX_LOG_TABLE + " partitions");
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

    private static void createGlobalTxLogTableV2(Statement stmt, long initTxid) throws SQLException {
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
        stmt.executeUpdate(String.format(CREATE_GLOBAL_TX_TABLE_V2, "p_" + initTxid, initTxid));
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

    public String getCurrentServerAddr() {
        return currentServerAddr;
    }
}
