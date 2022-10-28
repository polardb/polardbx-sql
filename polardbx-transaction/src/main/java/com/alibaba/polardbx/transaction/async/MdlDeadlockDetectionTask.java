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
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.transaction.DeadlockParser;
import com.alibaba.polardbx.executor.utils.transaction.GroupConnPair;
import com.alibaba.polardbx.executor.utils.transaction.LocalTransaction;
import com.alibaba.polardbx.executor.utils.transaction.TrxLock;
import com.alibaba.polardbx.executor.utils.transaction.TrxLookupSet;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.sync.FetchTransForDeadlockDetectionSyncAction;
import com.alibaba.polardbx.transaction.utils.DiGraph;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;

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
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.utils.transaction.TrxLookupSet.Transaction.FAKE_GROUP_FOR_DDL;
import static java.lang.Math.min;

/**
 * activates if these conditions are met:
 * 1. the performance_schema of DN is 'ON'
 * 2. CN has the select privilege of performance_schema.metadata_locks
 * 3. 'wait/lock/metadata/sql/mdl' is 'ON'
 * <p>
 * MdlDeadlockDetectionTask will detect performance_schema's select privilege on the fly
 * and may cancel itself if it don't have the privilege
 *
 * @author guxu
 */
public class MdlDeadlockDetectionTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MdlDeadlockDetectionTask.class);

    private final String db;

    private final TransactionExecutor executor;

    private static Class killSyncActionClass;

    private static final String SELECT_PERFORMANCE_SCHEMA_METALOCKS =
        "select "
            + "`g`.`OBJECT_SCHEMA` AS `object_schema`,"
            + "`g`.`OBJECT_NAME` AS `object_name`,"
            + "`pt`.`THREAD_ID` AS `waiting_thread_id`,"
            + "`pt`.`PROCESSLIST_ID` AS `waiting_pid`,"
            + "`sys`.`ps_thread_account`(`p`.`OWNER_THREAD_ID`) AS `waiting_account`,"
            + "`p`.`LOCK_TYPE` AS `waiting_lock_type`,"
            + "`p`.`LOCK_DURATION` AS `waiting_lock_duration`,"
            + "`pt`.`PROCESSLIST_INFO` AS `waiting_query`,"
            + "`pt`.`PROCESSLIST_TIME` AS `waiting_query_secs`,"
            + "`ps`.`ROWS_AFFECTED` AS `waiting_query_rows_affected`,"
            + "`ps`.`ROWS_EXAMINED` AS `waiting_query_rows_examined`,"
            + "`gt`.`THREAD_ID` AS `blocking_thread_id`,"
            + "`gt`.`PROCESSLIST_ID` AS `blocking_pid`,"
            + "`gt`.`PROCESSLIST_INFO` AS `blocking_query`,"
            + "`gt`.`PROCESSLIST_TIME` AS `blocking_query_secs`,"
            + "`sys`.`ps_thread_account`(`g`.`OWNER_THREAD_ID`) AS `blocking_account`,"
            + "`g`.`LOCK_TYPE` AS `blocking_lock_type`,"
            + "`g`.`LOCK_DURATION` AS `blocking_lock_duration`,"
            + "concat('KILL QUERY ',`gt`.`PROCESSLIST_ID`) AS `sql_kill_blocking_query`,"
            + "concat('KILL ',`gt`.`PROCESSLIST_ID`) AS `sql_kill_blocking_connection` "
            + "from (((((`performance_schema`.`metadata_locks` `g` "
            + "join `performance_schema`.`metadata_locks` `p` "
            + "on(((`g`.`OBJECT_TYPE` = `p`.`OBJECT_TYPE`) "
            + "and (`g`.`OBJECT_SCHEMA` = `p`.`OBJECT_SCHEMA`) "
            + "and (`g`.`OBJECT_NAME` = `p`.`OBJECT_NAME`) "
            + "and (`g`.`LOCK_STATUS` = 'GRANTED') "
            + "and (`p`.`LOCK_STATUS` = 'PENDING')))) "
            + "join `performance_schema`.`threads` `gt` "
            + "on((`g`.`OWNER_THREAD_ID` = `gt`.`THREAD_ID`))) "
            + "join `performance_schema`.`threads` `pt` "
            + "on((`p`.`OWNER_THREAD_ID` = `pt`.`THREAD_ID`))) "
            + "left join `performance_schema`.`events_statements_current` `gs` "
            + "on((`g`.`OWNER_THREAD_ID` = `gs`.`THREAD_ID`))) "
            + "left join `performance_schema`.`events_statements_current` `ps` "
            + "on((`p`.`OWNER_THREAD_ID` = `ps`.`THREAD_ID`))) "
            + "where "
            + "(`g`.`OBJECT_TYPE` = 'TABLE') "
            + "AND "
            + "`pt`.`PROCESSLIST_ID` != `gt`.`PROCESSLIST_ID`";

    /**
     * DDL will wait for MDL for some seconds, then kill the query which held the MDL
     */
    private long MDL_WAIT_TIMEOUT;

    static {
        // 只有server支持，这里是暂时改法，后续要将这段逻辑解耦
        try {
            killSyncActionClass =
                Class.forName("com.alibaba.polardbx.server.response.KillSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public MdlDeadlockDetectionTask(String db, TransactionExecutor executor, int mdlWaitTimeoutInSec) {
        this.db = db;
        this.executor = executor;
        this.MDL_WAIT_TIMEOUT = mdlWaitTimeoutInSec;
    }

    /**
     * fetch lock-wait information for this DN, and update
     * 1. the wait-for graph,
     * 2. the lookup set containing all transactions,
     * 3. the mysqlConn2GroupMap containing the mapping from DN connection id to a group data source
     *
     * @param dataSource is the data source of a DN
     * @param groupNames are names of all group in this DN
     * @param graph is updated in this method
     * @param lookupSet is updated in this method
     * @param mysqlConn2DatasourceMap is updated in this method
     * @param mdlWaitInfoList is updated in this method
     */
    public void fetchMetaDataLockWaits(TGroupDataSource dataSource,
                                       Collection<String> groupNames,
                                       DiGraph<TrxLookupSet.Transaction> graph,
                                       TrxLookupSet lookupSet,
                                       Map<Long, TGroupDataSource> mysqlConn2DatasourceMap,
                                       List<MdlWaitInfo> mdlWaitInfoList) {
        final Map<Long, TrxLookupSet.Transaction> ddlTrxMap = new HashMap<>();
        try (final IConnection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            final ResultSet rs = stmt.executeQuery(SELECT_PERFORMANCE_SCHEMA_METALOCKS);
            while (rs.next()) {
                // Get the waiting and blocking processlist id
                final long waiting = rs.getLong("waiting_pid");
                final long blocking = rs.getLong("blocking_pid");

                // Update mysqlConn2DatasourceMap
                mysqlConn2DatasourceMap.put(waiting, dataSource);
                mysqlConn2DatasourceMap.put(blocking, dataSource);

                final String waitingQuery = rs.getString("waiting_query");
                final long waitingQuerySecs = rs.getLong("waiting_query_secs");
                final String waitingLockType = rs.getString("waiting_lock_type");
                final String blockingLockType = rs.getString("blocking_lock_type");

                // Update mdlWaitInfoList, it is used to do the following thing:
                // If A is waiting for EXCLUSIVE MDL blocked by B, and B is holding a non-EXCLUSIVE MDL lock
                // Then we kill B
                if (waitingQuerySecs >= MDL_WAIT_TIMEOUT &&
                    StringUtils.equalsIgnoreCase(waitingLockType, "EXCLUSIVE")
                    && !StringUtils.equalsIgnoreCase(blockingLockType, "EXCLUSIVE")) {
                    MdlWaitInfo mdlInfo =
                        new MdlWaitInfo(waiting, blocking, waitingLockType, waitingQuerySecs, waitingQuery, dataSource);
                    mdlWaitInfoList.add(mdlInfo);
                }

                // The wait-block relation is not so accurate.
                // If A is waiting B, and B is waiting C, then we would detect A is waiting C.
                // In such case, if we put A->C in the graph, B may not appear in the final detected cycle.
                if (notBlockMdlType(waitingLockType, blockingLockType)) {
                    continue;
                }

                // Get the waiting and blocking transaction
                final Triple<TrxLookupSet.Transaction, TrxLookupSet.Transaction, String> waitingAndBlockingTrx =
                    lookupSet.getWaitingAndBlockingTrx(groupNames, waiting, blocking);

                // Update waiting trx/DDL
                TrxLookupSet.Transaction waitingTrx = waitingAndBlockingTrx.getLeft();
                LocalTransaction waitingLocalTrx;
                // If the waiting_pid corresponds to a DDL statement, its transaction is null
                if (null == waitingTrx) {
                    // Get the fake transaction for DDL
                    waitingTrx = ddlTrxMap.get(waiting);
                    if (null == waitingTrx) {
                        waitingTrx = new TrxLookupSet.Transaction(waiting);
                        ddlTrxMap.put(waiting, waitingTrx);
                        final Long startTime = System.currentTimeMillis() - waitingQuerySecs * 1000;
                        waitingTrx.setStartTime(startTime);
                    }

                    // Get the fake local transaction,
                    // Note that this fake transaction should have only one local transaction
                    waitingLocalTrx = waitingTrx.getLocalTransaction(FAKE_GROUP_FOR_DDL);
                } else {
                    final String groupName = waitingAndBlockingTrx.getRight();
                    waitingLocalTrx = waitingTrx.getLocalTransaction(groupName);
                }
                // Update other information of waiting trx
                if (null != waitingQuery) {
                    final String truncatedSql = waitingQuery.substring(0, min(waitingQuery.length(), 4096));
                    waitingLocalTrx.setPhysicalSql(truncatedSql);
                }

                final String physicalTable = String.format("`%s`.`%s`",
                    rs.getString("object_schema"), rs.getString("object_name"));
                if (null == waitingLocalTrx.getWaitingTrxLock()) {
                    // A lock id represents a unique lock,
                    // and we use {schema.table:lock type} to represent an MDL lock id
                    final String lockId = String.format("%s:%s", physicalTable, waitingLockType);
                    waitingLocalTrx.setWaitingTrxLock(new TrxLock(lockId, waitingLockType, physicalTable));
                }

                // Update blocking trx/DDL
                TrxLookupSet.Transaction blockingTrx = waitingAndBlockingTrx.getMiddle();
                LocalTransaction blockingLocalTrx;
                // If the blocking_pid corresponds to a DDL statement, its transaction is null
                if (null == blockingTrx) {
                    // Get the fake transaction for DDL
                    blockingTrx = ddlTrxMap.get(blocking);
                    if (null == blockingTrx) {
                        blockingTrx = new TrxLookupSet.Transaction(blocking);
                        ddlTrxMap.put(blocking, blockingTrx);
                    }

                    // Get the fake local transaction,
                    // Note that this fake transaction should have only one local transaction
                    blockingLocalTrx =
                        blockingTrx.getLocalTransaction(FAKE_GROUP_FOR_DDL);
                    final long blockingQuerySecs = rs.getLong("blocking_query_secs");
                    final Long startTime = System.currentTimeMillis() - blockingQuerySecs * 1000;
                    blockingTrx.setStartTime(startTime);
                } else {
                    final String groupName = waitingAndBlockingTrx.getRight();
                    blockingLocalTrx = blockingTrx.getLocalTransaction(groupName);
                }
                // Update other information of blocking trx
                final String blockingQuery = rs.getString("blocking_query");
                if (null != blockingQuery && null == blockingLocalTrx.getPhysicalSql()) {
                    final String truncatedSql = blockingQuery.substring(0, min(blockingQuery.length(), 4096));
                    blockingLocalTrx.setPhysicalSql(truncatedSql);
                }

                // A lock id represents a unique lock,
                // and we use {schema.table:lock type} to represent an MDL lock id
                final String lockId = String.format("%s:%s", physicalTable, blockingLockType);
                blockingLocalTrx.addHoldingTrxLock(new TrxLock(lockId, blockingLockType, physicalTable));
                graph.addDiEdge(waitingTrx, blockingTrx);
            }
        } catch (SQLException ex) {
            final String dnId = dataSource.getMasterSourceAddress();
            throw new RuntimeException(
                String.format("Failed to fetch lock waits on data source %s. msg:%s", dnId, ex.getMessage()), ex);
        }
    }

    /**
     * @return Whether the blocking lock should block the waiting lock.
     * For example, SHARED_WRITE should NOT block SHARED_WRITE.
     */
    private boolean notBlockMdlType(String waitingLockType, String blockingLockType) {
        final Set<String> notBlockMdlType = ImmutableSet.of("SHARED_WRITE", "SHARED_READ");
        return notBlockMdlType.contains(waitingLockType) && notBlockMdlType.contains(blockingLockType);
    }

    /**
     * Fetch all transactions on the given db
     */
    public TrxLookupSet fetchTransInfo(String db) {
        final TrxLookupSet lookupSet = new TrxLookupSet();
        final List<List<Map<String, Object>>> results =
            SyncManagerHelper.sync(new FetchTransForDeadlockDetectionSyncAction(db), db);
        for (List<Map<String, Object>> result : results) {
            if (result == null) {
                continue;
            }
            for (Map<String, Object> row : result) {
                final Long transId = (Long) row.get("TRANS_ID");
                final String group = (String) row.get("GROUP");
                final long connId = (Long) row.get("CONN_ID");
                final long frontendConnId = (Long) row.get("FRONTEND_CONN_ID");
                final Long startTime = (Long) row.get("START_TIME");
                final String sql = (String) row.get("SQL");
                final GroupConnPair entry = new GroupConnPair(group, connId);
                lookupSet.addNewTransaction(entry, transId);
                lookupSet.updateTransaction(transId, frontendConnId, sql, startTime);
            }
        }
        return lookupSet;
    }

    private void killByFrontendConnId(long frontendConnId) {
        ISyncAction killSyncAction;
        try {
            killSyncAction =
                (ISyncAction) killSyncActionClass.getConstructor(String.class, Long.TYPE, Boolean.TYPE, ErrorCode.class)
                    .newInstance(db, frontendConnId, true, ErrorCode.ERR_TRANS_DEADLOCK);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
        SyncManagerHelper.sync(killSyncAction);
    }

    private void killByBackendConnId(final Long backendConnId, final TGroupDataSource dataSource) {
        if (backendConnId == null || null == dataSource) {
            return;
        }
        try (IConnection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(String.format("kill %s", backendConnId));
        } catch (SQLException ex) {
            final String dnId = dataSource.getMasterSourceAddress();
            throw new RuntimeException("Failed to kill connection on datasource " + dnId, ex);
        }
    }

    @Override
    public void run() {

        boolean hasLeadership = ExecUtils.hasLeadership(db);

        if (!hasLeadership) {
            TransactionLogger.debug("Skip MDL deadlock detection task since I am not the leader");
            return;
        }
        logger.debug("MDL Deadlock detection task starts.");
        try {
            detectMetaDataDeadLock();
        } catch (Throwable ex) {
            if (StringUtils.containsIgnoreCase(ex.getMessage(), "denied")) {
                cancel();
            } else {
                logger.error("Failed to do MDL deadlock detection", ex);
            }
        }
    }

    private void detectMetaDataDeadLock() {
        final DiGraph<TrxLookupSet.Transaction> graph = new DiGraph<>();

        // Get all global transaction information
        final TrxLookupSet lookupSet = fetchTransInfo(db);

        // Get all group data sources, and group by DN's ID (host:port)
        final Map<String, List<TGroupDataSource>> instId2GroupList = ExecUtils.getInstId2GroupList(db);

        final Map<Long, TGroupDataSource> mysqlConn2DatasourceMap = new HashMap<>();
        List<MdlWaitInfo> mdlWaitInfoList = new ArrayList<>();

        // For each DN, find the lock-wait information and add it to the graph
        for (final List<TGroupDataSource> groupDataSources : instId2GroupList.values()) {
            if (CollectionUtils.isNotEmpty(groupDataSources)) {
                // Since all data sources are in the same DN, any data source is ok
                final TGroupDataSource groupDataSource = groupDataSources.get(0);

                // Get all group names in this DN
                final List<String> groupNames =
                    groupDataSources.stream().map(TGroupDataSource::getDbGroupKey).collect(Collectors.toList());

                // Fetch MDL-lock-wait information for this DN,
                // and update the graph, the lookup set, the mysqlConn2GroupMap, and the mdlWaitInfoList
                fetchMetaDataLockWaits(groupDataSource, groupNames,
                    graph, lookupSet, mysqlConn2DatasourceMap, mdlWaitInfoList);
            }
        }
        graph.detect().ifPresent((cycle) -> {
            assert cycle.size() >= 2;
            final Pair<StringBuilder, StringBuilder> deadlockLog = DeadlockParser.parseMdlDeadlock(cycle);
            final StringBuilder simpleDeadlockLog = deadlockLog.getKey();
            final StringBuilder fullDeadlockLog = deadlockLog.getValue();

            // ddlMap/trxMap: index in cycle -> transaction
            final Map<Integer, TrxLookupSet.Transaction> ddlMap = new HashMap<>();
            final Map<Integer, TrxLookupSet.Transaction> trxMap = new HashMap<>();

            for (int i = 0; i < cycle.size(); i++) {
                TrxLookupSet.Transaction trx = cycle.get(i);
                if (trx.isDdl()) {
                    ddlMap.put(i, trx);
                } else {
                    trxMap.put(i, trx);
                }
            }

            if (MapUtils.isNotEmpty(trxMap)) {
                // Choose a transaction to kill
                final Map.Entry<Integer, TrxLookupSet.Transaction> toKill = trxMap.entrySet().iterator().next();
                final Integer victimIndex = toKill.getKey();
                final TrxLookupSet.Transaction victimTrx = toKill.getValue();
                fullDeadlockLog.append(String.format("*** WE ROLL BACK TRANSACTION (%s)\n", victimIndex + 1));
                simpleDeadlockLog
                    .append(String.format(" Will rollback trx %s", Long.toHexString(victimTrx.getTransactionId())));

                // Kill transaction by frontend connection
                killByFrontendConnId(victimTrx.getFrontendConnId());
            } else if (MapUtils.isNotEmpty(ddlMap)) {
                // Choose a DDL to kill
                final Map.Entry<Integer, TrxLookupSet.Transaction> toKill = trxMap.entrySet().iterator().next();
                final Integer victimIndex = toKill.getKey();
                final TrxLookupSet.Transaction victimDdl = toKill.getValue();
                final Long ddlBackendConnId = victimDdl.getTransactionId();
                fullDeadlockLog.append(String.format("*** WE KILL DDL (%s)\n", victimIndex + 1));
                simpleDeadlockLog.append(
                    String.format(" Will kill ddl %s", Long.toHexString(ddlBackendConnId)));

                // Kill DDL by backend connection
                killByBackendConnId(ddlBackendConnId, mysqlConn2DatasourceMap.get(ddlBackendConnId));
            }

            TransactionLogger.warn(simpleDeadlockLog.toString());
            logger.warn(simpleDeadlockLog.toString());
            EventLogger.log(EventType.DEAD_LOCK_DETECTION, simpleDeadlockLog.toString());

            // Store deadlock log in StorageInfoManager so that executor can access it
            StorageInfoManager.updateMdlDeadlockInfo(fullDeadlockLog.toString());
        });

        if (!graph.detect().isPresent() && CollectionUtils.isNotEmpty(mdlWaitInfoList)) {
            for (MdlWaitInfo mdlWaitInfo : mdlWaitInfoList) {
                final String dnId = mdlWaitInfo.dataSource.getMasterSourceAddress();
                final String killMessage = String.format(
                    "metadata lock wait time out: DN(%s) connection %s is trying to acquire EXCLUSIVE MDL lock, "
                        + "but blocked by connection %s. try kill %s",
                    dnId, mdlWaitInfo.waiting, mdlWaitInfo.blocking, mdlWaitInfo.blocking);
                logger.warn(killMessage);
                EventLogger.log(EventType.DEAD_LOCK_DETECTION, killMessage);
                killByBackendConnId(mdlWaitInfo.blocking, mdlWaitInfo.dataSource);
            }
        }
    }

    /**
     * cancel current MDL Detection task
     */
    private void cancel() {
        logger.error(String.format("cancel MDL Deadlock Detection Task for schema: %s for lack of privilege", db));
        EventLogger.log(EventType.DEAD_LOCK_DETECTION, String.format(
            "MDL Deadlock detection task for schema:%s is offline",
            db
        ));
        TransactionManager manager = TransactionManager.getInstance(db);
        if (manager != null) {
            manager.cancelMdlDeadlockDetection();
        }
    }

    private static class MdlWaitInfo {
        long waiting;
        long blocking;
        String waitingLockType;
        long waitingQuerySecs;
        String waitingQuery;
        TGroupDataSource dataSource;

        public MdlWaitInfo(final long waiting,
                           final long blocking,
                           final String waitingLockType,
                           final long waitingQuerySecs,
                           final String waitingQuery,
                           final TGroupDataSource dataSource) {
            this.waiting = waiting;
            this.blocking = blocking;
            this.waitingLockType = waitingLockType;
            this.waitingQuerySecs = waitingQuerySecs;
            this.waitingQuery = waitingQuery;
            this.dataSource = dataSource;
        }
    }
}
