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

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.mock.MockStatus;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.transaction.DeadlockParser;
import com.alibaba.polardbx.executor.utils.transaction.LocalTransaction;
import com.alibaba.polardbx.executor.utils.transaction.TransactionUtils;
import com.alibaba.polardbx.executor.utils.transaction.TrxLock;
import com.alibaba.polardbx.executor.utils.transaction.TrxLookupSet;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.trx.DeadlocksAccessor;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;
import com.alibaba.polardbx.transaction.sync.FetchTransForDeadlockDetectionSyncAction;
import com.alibaba.polardbx.transaction.utils.DiGraph;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;

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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;
import static java.lang.Math.min;

/**
 * Deadlock detection task.
 *
 * @author TennyZhuang
 */
public class DeadlockDetectionTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DeadlockDetectionTask.class);

    /**
     * Query all unique lock and how many trx are waiting for them respectively.
     */
    protected static final String SQL_QUERY_HOTSPOT_LOCK =
        "SELECT "
            + "COUNT(0) AS cnt, SUBSTRING_INDEX(TRX_REQUESTED_LOCK_ID, ':', -3) AS lock_id "
            + "FROM INFORMATION_SCHEMA.innodb_trx "
            + "GROUP BY lock_id";

    protected static final String SQL_QUERY_HOTSPOT_LOCK_80 =
        "SELECT "
            + "COUNT(0) AS cnt, SUBSTRING_INDEX(SUBSTRING_INDEX(TRX_REQUESTED_LOCK_ID, ':', -4), ':', 3) AS lock_id "
            + "FROM INFORMATION_SCHEMA.innodb_trx "
            + "GROUP BY lock_id";

    /**
     * Query all local trx, including waiting and blocking trx.
     */
    protected static final String SQL_QUERY_TRX_80 =
        "SELECT "
            + "TRX_ID AS trx_id, "
            + "TRX_MYSQL_THREAD_ID AS conn_id, "
            + "TRX_STATE AS state, "
            + "TRX_QUERY AS physical_sql, "
            + "TRX_OPERATION_STATE AS operation_state, "
            + "TRX_TABLES_IN_USE AS tables_in_use, "
            + "TRX_TABLES_LOCKED AS tables_locked, "
            + "TRX_LOCK_STRUCTS AS lock_structs, "
            + "TRX_LOCK_MEMORY_BYTES AS heap_size, "
            + "TRX_ROWS_LOCKED AS row_locks "
            + "FROM information_schema.INNODB_TRX ";

    /**
     * Query all blocking and waiting trx.
     */
    protected static final String SQL_QUERY_LOCK_WAITS_80 =
        "SELECT "
            + "REQUESTING_ENGINE_TRANSACTION_ID AS waiting_trx_id, "
            + "BLOCKING_ENGINE_TRANSACTION_ID AS blocking_trx_id "
            + "FROM "
            + "performance_schema.DATA_LOCK_WAITS AS lock_waits ";

    protected static final String SQL_QUERY_DEADLOCKS =
        "SELECT "
            /* Transaction information of waiting transaction. */
            + "trx_a.TRX_MYSQL_THREAD_ID AS waiting_conn_id, "
            + "trx_a.TRX_STATE AS waiting_state, "
            + "trx_a.TRX_QUERY AS waiting_physical_sql, "
            + "trx_a.TRX_OPERATION_STATE AS waiting_operation_state, "
            + "trx_a.TRX_TABLES_IN_USE AS waiting_tables_in_use, "
            + "trx_a.TRX_TABLES_LOCKED AS waiting_tables_locked, "
            + "trx_a.TRX_LOCK_STRUCTS AS waiting_lock_structs, "
            + "trx_a.TRX_LOCK_MEMORY_BYTES AS waiting_heap_size, "
            + "trx_a.TRX_ROWS_LOCKED AS waiting_row_locks, "
            /* Lock information of waiting transaction. */
            + "locks_a.LOCK_ID AS waiting_lock_id, "
            + "locks_a.LOCK_MODE AS waiting_lock_mode, "
            + "locks_a.LOCK_TYPE AS waiting_lock_type, "
            + "locks_a.LOCK_TABLE AS waiting_lock_physical_table, "
            + "locks_a.LOCK_INDEX AS waiting_lock_index, "
            + "locks_a.LOCK_SPACE AS waiting_lock_space, "
            + "locks_a.LOCK_PAGE AS waiting_lock_page, "
            + "locks_a.LOCK_REC AS waiting_lock_rec, "
            + "locks_a.LOCK_DATA AS waiting_lock_data, "
            /* Transaction information of blocking transaction. */
            + "trx_b.TRX_MYSQL_THREAD_ID AS blocking_conn_id, "
            + "trx_b.TRX_STATE AS blocking_state, "
            + "trx_b.TRX_QUERY AS blocking_physical_sql, "
            + "trx_b.TRX_OPERATION_STATE AS blocking_operation_state, "
            + "trx_b.TRX_TABLES_IN_USE AS blocking_tables_in_use, "
            + "trx_b.TRX_TABLES_LOCKED AS blocking_tables_locked, "
            + "trx_b.TRX_LOCK_STRUCTS AS blocking_lock_structs, "
            + "trx_b.TRX_LOCK_MEMORY_BYTES AS blocking_heap_size, "
            + "trx_b.TRX_ROWS_LOCKED AS blocking_row_locks, "
            /* Lock information of blocking transaction. */
            + "locks_b.LOCK_ID AS blocking_lock_id, "
            + "locks_b.LOCK_MODE AS blocking_lock_mode, "
            + "locks_b.LOCK_TYPE AS blocking_lock_type, "
            + "locks_b.LOCK_TABLE AS blocking_lock_physical_table, "
            + "locks_b.LOCK_INDEX AS blocking_lock_index, "
            + "locks_b.LOCK_SPACE AS blocking_lock_space, "
            + "locks_b.LOCK_PAGE AS blocking_lock_page, "
            + "locks_b.LOCK_REC AS blocking_lock_rec, "
            + "locks_b.LOCK_DATA AS blocking_lock_data "
            + "FROM "
            + "information_schema.INNODB_LOCK_WAITS AS lock_waits, "
            /* trx_a requesting locks_a, is blocked by trx_b holding locks_b */
            + "information_schema.INNODB_TRX AS trx_a, information_schema.INNODB_TRX AS trx_b, "
            + "information_schema.INNODB_LOCKS AS locks_a, information_schema.INNODB_LOCKS AS locks_b "
            + "WHERE "
            /* Filter the non-direct-blocking trx. Trx which is waiting a lock and locked rows <= 1 should not block others. */
            + "(trx_b.trx_state != 'LOCK WAIT' OR trx_b.trx_rows_locked > 1) "
            + "AND "
            /* Join innodb_trx to get the trx information. */
            + "trx_b.trx_id = lock_waits.blocking_trx_id AND lock_waits.requesting_trx_id = trx_a.trx_id "
            + "AND "
            /* Join innodb_locks to get the lock information. */
            + "lock_waits.requested_lock_id = locks_a.lock_id AND lock_waits.blocking_lock_id = locks_b.lock_id";

    private static final AtomicLong SKIP = new AtomicLong(0L);
    private final Collection<String> allSchemas;
    private static Class killSyncActionClass;

    private static boolean debug = false;

    static {
        // 只有server支持
        try {
            if (MockStatus.isMock()) {
                killSyncActionClass = null;
            } else {
                killSyncActionClass =
                    Class.forName("com.alibaba.polardbx.server.response.KillSyncAction");
            }
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public DeadlockDetectionTask(Collection<String> allSchemas) {
        this.allSchemas = allSchemas;
    }

    /**
     * fetch lock-wait information to update the wait-for graph and the lookup set
     *
     * @param dataSource a DN's data source
     * @param groupNames all group on that DN
     * @param lookupSet a transaction lookup set which contains all transactions
     * @param graph a wait-for graph containing "trx a is waiting for trx b"-like information
     */
    protected void fetchLockWaits(TGroupDataSource dataSource,
                                  Collection<String> groupNames,
                                  TrxLookupSet lookupSet,
                                  DiGraph<TrxLookupSet.Transaction> graph) {
        if (InstanceVersion.isMYSQL80()) {
            // Do join in CN.
            // local trx id -> local trx
            Map<Long, LocalTrxInfo> localTrxInfoMap = new HashMap<>();
            long maxFetchRows = DynamicConfig.getInstance().getDeadlockDetection80FetchTrxRows();

            // 1. Fetch all local trx info.
            maxFetchRows = Math.max(maxFetchRows, 1L);
            Set<Long> blockingTrxSet = new HashSet<>();
            try (final Connection conn = createPhysicalConnectionForLeaderStorage(dataSource);
                final Statement stmt = conn.createStatement();
                final ResultSet rs = stmt.executeQuery(SQL_QUERY_TRX_80 + " LIMIT " + maxFetchRows)) {
                StringBuilder sb = new StringBuilder("all innodb trx: ");
                while (rs.next()) {
                    LocalTrxInfo localTrxInfo = new LocalTrxInfo(
                        rs.getLong("trx_id"),
                        rs.getLong("conn_id"),
                        rs.getString("state"),
                        rs.getString("physical_sql"),
                        rs.getString("operation_state"),
                        rs.getInt("tables_in_use"),
                        rs.getInt("tables_locked"),
                        rs.getInt("lock_structs"),
                        rs.getInt("heap_size"),
                        rs.getInt("row_locks")
                    );
                    if (debug) {
                        sb.append("\n").append(localTrxInfo.info());
                    }
                    localTrxInfoMap.put(localTrxInfo.getTrxId(), localTrxInfo);
                    if ((!localTrxInfo.getState().equalsIgnoreCase("LOCK WAIT")
                        && localTrxInfo.getRowLocks() > 0)
                        || localTrxInfo.getRowLocks() > 1) {
                        blockingTrxSet.add(localTrxInfo.getTrxId());
                    }
                }
                if (debug) {
                    logger.warn(sb.toString());
                    logger.warn("all blocking trx: ");
                    blockingTrxSet.forEach(b -> logger.warn(String.valueOf(b)));
                }
            } catch (SQLException ex) {
                final String dnId = dataSource.getMasterSourceAddress();
                throw new RuntimeException("Failed to fetch trx info on data source " + dnId, ex);
            }

            // 2. Fetch waiting trx info.
            if (!blockingTrxSet.isEmpty()) {
                try (final Connection conn = createPhysicalConnectionForLeaderStorage(dataSource);
                    final Statement stmt = conn.createStatement();
                    final ResultSet rs = stmt.executeQuery(SQL_QUERY_LOCK_WAITS_80)) {
                    StringBuilder sb = new StringBuilder("all wait-for trx: ");
                    while (rs.next()) {
                        final long waitingTrxId = rs.getLong("waiting_trx_id");
                        final long blockingTrxId = rs.getLong("blocking_trx_id");
                        if (debug) {
                            sb.append("\ntrx ")
                                .append(waitingTrxId)
                                .append(" waiting ")
                                .append(blockingTrxId);
                        }
                        if (!blockingTrxSet.contains(blockingTrxId)) {
                            if (debug) {
                                sb.append("\ntrx ").append(blockingTrxId).append(" not found in blocking list");
                            }
                            continue;
                        }
                        final LocalTrxInfo waitingTrxInfo = localTrxInfoMap.get(waitingTrxId);
                        final LocalTrxInfo blockingTrxInfo = localTrxInfoMap.get(blockingTrxId);
                        if (null == waitingTrxInfo || null == blockingTrxInfo) {
                            if (debug) {
                                if (null == waitingTrxInfo) {
                                    sb.append("\nwaiting trx ").append(waitingTrxId).append(" not found in local");
                                } else {
                                    sb.append("\nblocking trx ").append(blockingTrxId).append(" not found in local");
                                }
                            }
                            continue;
                        }
                        // Update the wait-for graph and the lookup set
                        // Get the waiting and blocking transaction
                        final long waiting = waitingTrxInfo.getConnId();
                        final long blocking = blockingTrxInfo.getConnId();
                        final Triple<TrxLookupSet.Transaction, TrxLookupSet.Transaction, String> waitingAndBlockingTrx =
                            lookupSet.getWaitingAndBlockingTrx(groupNames, waiting, blocking);
                        final TrxLookupSet.Transaction waitingTrx = waitingAndBlockingTrx.getLeft();
                        final TrxLookupSet.Transaction blockingTrx = waitingAndBlockingTrx.getMiddle();

                        if (null != waitingTrx && null != blockingTrx) {
                            if (debug) {
                                sb.append("\n").append("trx ")
                                    .append(waitingTrxId)
                                    .append(" trx id ")
                                    .append(Long.toHexString(waitingTrx.getTransactionId()))
                                    .append(" waiting trx ")
                                    .append(blockingTrxId)
                                    .append(" trx id ")
                                    .append(Long.toHexString(blockingTrx.getTransactionId()));
                            }
                            // Update the wait-for graph and the lookup set
                            graph.addDiEdge(waitingTrx, blockingTrx);

                            try {
                                // Get the group which the waiting and blocking thread id are in
                                final String groupName = waitingAndBlockingTrx.getRight();

                                // Get the waiting local transaction of this group
                                final LocalTransaction waitingLocalTrx = waitingTrx.getLocalTransaction(groupName);
                                extractTrx80(waitingTrxInfo, waitingLocalTrx);

                                // Get the blocking local transaction of this group
                                final LocalTransaction blockingLocalTrx = blockingTrx.getLocalTransaction(groupName);
                                extractTrx80(blockingTrxInfo, blockingLocalTrx);
                            } catch (Throwable t) {
                                // Ignore.
                                logger.warn("Get lock-wait message failed.", t);
                            }
                        } else if (debug) {
                            if (null != waitingTrx) {
                                sb.append("\nFound single waiting trx ")
                                    .append(Long.toHexString(waitingTrx.getTransactionId())).append(" trx id ")
                                    .append(waitingTrxId).append(" blocking trx id ").append(blockingTrxId);
                            } else if (null != blockingTrx) {
                                sb.append("\nFound single blocking trx ")
                                    .append(Long.toHexString(blockingTrx.getTransactionId())).append(" trx id ")
                                    .append(blockingTrxId).append(" waiting trx id ").append(waitingTrxId);
                            } else {
                                sb.append("\nFound no polardbx trx waiting ").append(waitingTrxId).append(" blocking ")
                                    .append(blockingTrxId);
                            }
                        }
                    }
                    if (debug) {
                        logger.warn(sb.toString());
                    }
                } catch (SQLException ex) {
                    final String dnId = dataSource.getMasterSourceAddress();
                    throw new RuntimeException("Failed to fetch lock waits on data source " + dnId, ex);
                }
            }
        } else {
            try (final Connection conn = createPhysicalConnectionForLeaderStorage(dataSource);
                final Statement stmt = conn.createStatement();
                final ResultSet rs = stmt.executeQuery(SQL_QUERY_DEADLOCKS)) {

                while (rs.next()) {
                    // Get the waiting and blocking connection id of DN
                    final long waiting = rs.getLong("waiting_conn_id");
                    final long blocking = rs.getLong("blocking_conn_id");

                    // Get the waiting and blocking transaction
                    final Triple<TrxLookupSet.Transaction, TrxLookupSet.Transaction, String> waitingAndBlockingTrx =
                        lookupSet.getWaitingAndBlockingTrx(groupNames, waiting, blocking);

                    final TrxLookupSet.Transaction waitingTrx = waitingAndBlockingTrx.getLeft();
                    final TrxLookupSet.Transaction blockingTrx = waitingAndBlockingTrx.getMiddle();

                if (null != waitingTrx && null != blockingTrx) {
                    // Update the wait-for graph and the lookup set
                    graph.addDiEdge(waitingTrx, blockingTrx);
                    try {
                        // Get the group which the waiting and blocking thread id are in
                        final String groupName = waitingAndBlockingTrx.getRight();

                            // Get the waiting local transaction of this group
                            final LocalTransaction waitingLocalTrx = waitingTrx.getLocalTransaction(groupName);
                            extractWaitingTrx(rs, waitingLocalTrx);

                            // Get the blocking local transaction of this group
                            final LocalTransaction blockingLocalTrx = blockingTrx.getLocalTransaction(groupName);
                            extractBlockingTrx(rs, blockingLocalTrx);
                        } catch (Throwable t) {
                            // Ignore.
                            logger.warn("Get lock-wait message failed.", t);
                        }
                    }
                }
            } catch (SQLException ex) {
                final String dnId = dataSource.getMasterSourceAddress();
                throw new RuntimeException("Failed to fetch lock waits on data source " + dnId, ex);
            }
        }

    }

    private void extractBlockingTrx(ResultSet rs, LocalTransaction blockingLocalTrx)
        throws SQLException {
        if (!blockingLocalTrx.isUpdated()) {
            blockingLocalTrx.setState(rs.getString("blocking_state"));
            final String physicalSql = rs.getString("blocking_physical_sql");
            // Record a truncated SQL
            blockingLocalTrx.setPhysicalSql(
                physicalSql == null ? null : physicalSql.substring(0, min(4096, physicalSql.length())));
            blockingLocalTrx.setOperationState(rs.getString("blocking_operation_state"));
            blockingLocalTrx.setTablesInUse(rs.getInt("blocking_tables_in_use"));
            blockingLocalTrx.setTablesLocked(rs.getInt("blocking_tables_locked"));
            blockingLocalTrx.setLockStructs(rs.getInt("blocking_lock_structs"));
            blockingLocalTrx.setHeapSize(rs.getInt("blocking_heap_size"));
            blockingLocalTrx.setRowLocks(rs.getInt("blocking_row_locks"));

            blockingLocalTrx.setUpdated(true);
        }
        // Update holding-lock information of blocking transaction
        blockingLocalTrx.addHoldingTrxLock(new TrxLock(
            rs.getString("blocking_lock_id"),
            rs.getString("blocking_lock_mode"),
            rs.getString("blocking_lock_type"),
            rs.getString("blocking_lock_physical_table"),
            rs.getString("blocking_lock_index"),
            rs.getInt("blocking_lock_space"),
            rs.getInt("blocking_lock_page"),
            rs.getInt("blocking_lock_rec"),
            rs.getString("blocking_lock_data")
        ));
    }

    private void extractWaitingTrx(ResultSet rs, LocalTransaction waitingLocalTrx)
        throws SQLException {
        if (!waitingLocalTrx.isUpdated()) {
            waitingLocalTrx.setState(rs.getString("waiting_state"));
            final String physicalSql = rs.getString("waiting_physical_sql");
            // Record a truncated SQL
            waitingLocalTrx.setPhysicalSql(
                physicalSql == null ? null : physicalSql.substring(0, min(4096, physicalSql.length())));
            waitingLocalTrx.setOperationState(rs.getString("waiting_operation_state"));
            waitingLocalTrx.setTablesInUse(rs.getInt("waiting_tables_in_use"));
            waitingLocalTrx.setTablesLocked(rs.getInt("waiting_tables_locked"));
            waitingLocalTrx.setLockStructs(rs.getInt("waiting_lock_structs"));
            waitingLocalTrx.setHeapSize(rs.getInt("waiting_heap_size"));
            waitingLocalTrx.setRowLocks(rs.getInt("waiting_row_locks"));

            waitingLocalTrx.setUpdated(true);
        }

        if (null == waitingLocalTrx.getWaitingTrxLock()) {
            waitingLocalTrx.setWaitingTrxLock(new TrxLock(
                rs.getString("waiting_lock_id"),
                rs.getString("waiting_lock_mode"),
                rs.getString("waiting_lock_type"),
                rs.getString("waiting_lock_physical_table"),
                rs.getString("waiting_lock_index"),
                rs.getInt("waiting_lock_space"),
                rs.getInt("waiting_lock_page"),
                rs.getInt("waiting_lock_rec"),
                rs.getString("waiting_lock_data")
            ));
        }
    }

    private void extractTrx80(LocalTrxInfo info, LocalTransaction waitingLocalTrx) {
        if (!waitingLocalTrx.isUpdated()) {
            waitingLocalTrx.setState(info.getState());
            final String physicalSql = info.getPhysicalSql();
            // Record a truncated SQL
            waitingLocalTrx.setPhysicalSql(
                physicalSql == null ? null : physicalSql.substring(0, min(4096, physicalSql.length())));
            waitingLocalTrx.setOperationState(info.getOperationState());
            waitingLocalTrx.setTablesInUse(info.getTablesInUse());
            waitingLocalTrx.setTablesLocked(info.getTablesLocked());
            waitingLocalTrx.setLockStructs(info.getLockStructs());
            waitingLocalTrx.setHeapSize(info.getHeapSize());
            waitingLocalTrx.setRowLocks(info.getRowLocks());
            waitingLocalTrx.setUpdated(true);
        }
    }

    /**
     * Fetch all transactions on the instance.
     */
    public static TrxLookupSet fetchTransInfo() {
        final TrxLookupSet lookupSet = new TrxLookupSet();
        final List<List<Map<String, Object>>> results =
            SyncManagerHelper.sync(new FetchTransForDeadlockDetectionSyncAction(null), DEFAULT_DB_NAME,
                SyncScope.CURRENT_ONLY);
        TransactionUtils.updateTrxLookupSet(results, lookupSet);
        return lookupSet;
    }

    private void killByFrontendConnId(long frontendConnId) {
        ISyncAction killSyncAction;
        try {
            killSyncAction =
                (ISyncAction) killSyncActionClass
                    .getConstructor(String.class, Long.TYPE, Boolean.TYPE, Boolean.TYPE, ErrorCode.class)
                    // KillSyncAction(String user, long id, boolean killQuery, boolean skipValidation, ErrorCode cause)
                    .newInstance("", frontendConnId, true, true, ErrorCode.ERR_TRANS_DEADLOCK);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
        SyncManagerHelper.sync(killSyncAction, DEFAULT_DB_NAME, SyncScope.CURRENT_ONLY);
    }

    @Override
    public void run() {
        if (!hasLeadership()) {
            TransactionLogger.debug("Skip deadlock detection task since I am not the leader "
                + "or there are no active schemas.");
            return;
        }

        if (InstanceVersion.isMYSQL80() && !InstConfUtil.getBool(ConnectionParams.ENABLE_DEADLOCK_DETECTION_80)) {
            return;
        }

        if (SKIP.get() > 0) {
            SKIP.decrementAndGet();
            return;
        }

        debug = DynamicConfig.getInstance().isPrintMoreInfoForDeadlockDetection();

        TransactionLogger.debug("Deadlock detection task starts.");
        try {

            // Get all global transaction information
            final TrxLookupSet lookupSet = fetchTransInfo();

            // Get all group data sources, and group by DN's ID (host:port)
            final Map<String, List<TGroupDataSource>> instId2GroupList = ExecUtils.getInstId2GroupList(allSchemas);

            final DiGraph<TrxLookupSet.Transaction> graph = new DiGraph<>();

            // For each DN, find the lock-wait information and add it to the graph
            for (List<TGroupDataSource> groupDataSources : instId2GroupList.values()) {
                if (CollectionUtils.isNotEmpty(groupDataSources)) {
                    // Since all data sources are in the same DN, any data source is ok
                    final TGroupDataSource groupDataSource = groupDataSources.get(0);

                    if (StringUtils.containsIgnoreCase(groupDataSource.getMasterDNId(), "pxc-xdb-m-")) {
                        // Skip meta-db.
                        continue;
                    }

                    // Get all group names in this DN
                    final Set<String> groupNames =
                        groupDataSources.stream().map(TGroupDataSource::getDbGroupKey).collect(Collectors.toSet());

                    if (maybeTooManyDataLockWaits(groupDataSource)) {
                        return;
                    }

                    if (debug) {
                        logger.warn("trx lookup set: " + lookupSet);
                    }

                    // Fetch lock-wait information for this DN,
                    // and update the lookup set and the graph with the information
                    fetchLockWaits(groupDataSource, groupNames, lookupSet, graph);
                }
            }

            while (true) {
                AtomicBoolean detected = new AtomicBoolean(false);
                graph.detect().ifPresent((cycle) -> {
                    detected.set(true);
                    final Pair<StringBuilder, StringBuilder> deadlockLog = DeadlockParser.parseGlobalDeadlock(cycle);
                    final StringBuilder simpleDeadlockLog = deadlockLog.getKey();
                    final StringBuilder fullDeadlockLog = deadlockLog.getValue();

                    Optional.ofNullable(OptimizerContext.getTransStat(DEFAULT_DB_NAME))
                        .ifPresent(s -> s.countGlobalDeadlock.incrementAndGet());

                    // TODO: kill transaction by some priority, such as create time, or prefer to kill internal transaction.
                    // The index of the transaction to be killed in the cycle
                    int indexOfToKillTrx = 0;
                    for (int i = 0; i < cycle.size(); i++) {
                        if (!cycle.get(i).isDdl()) {
                            indexOfToKillTrx = i;
                        }
                    }

                    final TrxLookupSet.Transaction toKillTrx = cycle.get(indexOfToKillTrx);
                    simpleDeadlockLog
                        .append(String.format(" Will rollback %s", Long.toHexString(toKillTrx.getTransactionId())));
                    fullDeadlockLog.append(String.format("*** WE ROLL BACK TRANSACTION (%s)\n", indexOfToKillTrx + 1));

                    if (cycle.get(indexOfToKillTrx).isDdl()) {
                        printDdlDeadlock(cycle, fullDeadlockLog);
                    }

                    // Store deadlock log in StorageInfoManager so that executor can access it
                    StorageInfoManager.updateDeadlockInfo(fullDeadlockLog.toString());
                    // Record deadlock in meta db.
                    try (Connection connection = MetaDbUtil.getConnection()) {
                        DeadlocksAccessor deadlocksAccessor = new DeadlocksAccessor();
                        deadlocksAccessor.setConnection(connection);
                        deadlocksAccessor.recordDeadlock(GlobalTxLogManager.getCurrentServerAddr(), "GLOBAL",
                            fullDeadlockLog.toString());
                    } catch (Exception e) {
                        logger.error(e);
                    }

                    TransactionLogger.warn(simpleDeadlockLog.toString());
                    logger.warn(simpleDeadlockLog.toString());
                    EventLogger.log(EventType.DEAD_LOCK_DETECTION, simpleDeadlockLog.toString());

                    final long toKillFrontendConnId = toKillTrx.getFrontendConnId();
                    killByFrontendConnId(toKillFrontendConnId);

                    graph.removeEdge(toKillTrx);
                });
                if (!detected.get()) {
                    break;
                }
            }

        } catch (Throwable ex) {
            logger.error("Failed to do deadlock detection", ex);
        }
    }

    static void printDdlDeadlock(ArrayList<TrxLookupSet.Transaction> cycle, StringBuilder fullDeadlockLog) {
        TransactionLogger.warn("Deadlock caused by DDL, killing DDL.");
        logger.warn("Deadlock caused by DDL, killing DDL.");
        EventLogger.log(EventType.DEAD_LOCK_DETECTION, "Deadlock caused by DDL, killing DDL.");
        for (TrxLookupSet.Transaction transaction : cycle) {
            logger.warn(transaction.toString());
        }
        logger.warn(fullDeadlockLog.toString());
    }

    protected boolean maybeTooManyDataLockWaits(TGroupDataSource dataSource) {
        // Estimate row count of data_lock_waits records, if too many, skip this round of detection.
        // Or it may cause DN hang for a long time.
        String sql = InstanceVersion.isMYSQL80() ? SQL_QUERY_HOTSPOT_LOCK_80 : SQL_QUERY_HOTSPOT_LOCK;
        long estimateRowCount = 0;
        try (final Connection conn = createPhysicalConnectionForLeaderStorage(dataSource);
            final Statement stmt = conn.createStatement();
            final ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                String lockId = rs.getString("lock_id");
                if (null != lockId && !"NULL".equalsIgnoreCase(lockId)) {
                    long cnt = rs.getLong("cnt");
                    estimateRowCount += cnt * (cnt - 1) / 2;
                }
                if (estimateRowCount > DynamicConfig.getInstance().getDeadlockDetectionDataLockWaitsThreshold()) {
                    final String dnId = dataSource.getMasterSourceAddress();
                    String errMsg = "[" + dnId + "] Too many data_lock_waits records: " + estimateRowCount
                        + ", skip deadlock detection task.";
                    TransactionLogger.warn(errMsg);
                    EventLogger.log(EventType.DEAD_LOCK_DETECTION, errMsg);
                    SKIP.set(DynamicConfig.getInstance().getDeadlockDetectionSkipRound());
                    return true;
                }
            }
        } catch (SQLException ex) {
            final String dnId = dataSource.getMasterSourceAddress();
            throw new RuntimeException("Failed to estimate row count on data source " + dnId, ex);
        }
        return false;
    }

    private boolean hasLeadership() {
        return !allSchemas.isEmpty() && ExecUtils.hasLeadership(allSchemas.iterator().next());
    }

    public static Connection createPhysicalConnectionForLeaderStorage(TGroupDataSource dataSource) {
        String masterDnId = dataSource.getMasterDNId();
        return DbTopologyManager.getConnectionForStorage(masterDnId);
    }

    @Data
    private static class LocalTrxInfo {
        private final Long trxId;
        private final Long connId;
        private final String state;
        private final String physicalSql;
        private final String operationState;
        private final Integer tablesInUse;
        private final Integer tablesLocked;
        private final Integer lockStructs;
        private final Integer heapSize;
        private final Integer rowLocks;

        public LocalTrxInfo(Long trxId, Long connId, String state, String physicalSql, String operationState,
                            Integer tablesInUse, Integer tablesLocked, Integer lockStructs, Integer heapSize,
                            Integer rowLocks) {
            this.trxId = trxId;
            this.connId = connId;
            this.state = state;
            this.physicalSql = physicalSql;
            this.operationState = operationState;
            this.tablesInUse = tablesInUse;
            this.tablesLocked = tablesLocked;
            this.lockStructs = lockStructs;
            this.heapSize = heapSize;
            this.rowLocks = rowLocks;
        }

        public String info() {
            return "{\n"
                + " trxId: " + trxId + ",\n"
                + " connId: " + connId + ",\n"
                + " state: " + state + ",\n"
                + " rowLocks: " + rowLocks + ",\n"
                + " sql: " + physicalSql + "\n"
                + "}";
        }
    }
}
