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
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.transaction.DeadlockParser;
import com.alibaba.polardbx.executor.utils.transaction.GroupConnPair;
import com.alibaba.polardbx.executor.utils.transaction.LocalTransaction;
import com.alibaba.polardbx.executor.utils.transaction.TrxLock;
import com.alibaba.polardbx.executor.utils.transaction.TrxLookupSet;
import com.alibaba.polardbx.gms.metadb.trx.DeadlocksAccessor;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.log.GlobalTxLogManager;
import com.alibaba.polardbx.transaction.utils.DiGraph;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.utils.transaction.TrxLookupSet.Transaction.FAKE_GROUP_FOR_DDL;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;
import static com.alibaba.polardbx.gms.util.GroupInfoUtil.buildGroupNameFromPhysicalDb;
import static com.alibaba.polardbx.gms.util.GroupInfoUtil.buildPhysicalDbNameFromGroupName;

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

    private static final Map<String, AtomicLong> runningCountByDb = new ConcurrentHashMap<>();

    private final String db;

    private final TransactionExecutor executor;

    private static Class killSyncActionClass;

    private static final String PENDING_STATUS = "PENDING";

    private static final String GRANTED_STATUS = "GRANTED";

    public static final Long MIN_MDL_WAIT_TIMEOUT_SECOND = 2L;

    private Collection<String> allSchema;


    private static final Set<String> DDL_WAIT_MDL_LOCK_TYPE =
        ImmutableSet.of(
            "EXCLUSIVE", // for general online ddl: add column, drop column
            "SHARED_NO_READ_WRITE", // for optimize table, add index.
            "SHARED_NO_WRITE" // for modify column, modify partition.
        );
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
            + "`gt`.`THREAD_ID` AS `blocking_thread_id`,"
            + "`gt`.`PROCESSLIST_ID` AS `blocking_pid`,"
            + "`gt`.`PROCESSLIST_INFO` AS `blocking_query`,"
            + "`gt`.`PROCESSLIST_TIME` AS `blocking_query_secs`,"
            + "`sys`.`ps_thread_account`(`g`.`OWNER_THREAD_ID`) AS `blocking_account`,"
            + "`g`.`LOCK_TYPE` AS `blocking_lock_type`,"
            + "`g`.`LOCK_DURATION` AS `blocking_lock_duration`,"
            + "concat('KILL QUERY ',`gt`.`PROCESSLIST_ID`) AS `sql_kill_blocking_query`,"
            + "concat('KILL ',`gt`.`PROCESSLIST_ID`) AS `sql_kill_blocking_connection` "
            + "from (((`performance_schema`.`metadata_locks` `g` "
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
            + "where "
            + "(`g`.`OBJECT_TYPE` = 'TABLE') "
            + "AND "
            + "(`pt`.`PROCESSLIST_ID` != `gt`.`PROCESSLIST_ID`)";

    private static final String SELECT_MDL_WAITING_ONLY =
        "select "
            + "`g`.`OBJECT_SCHEMA` AS `object_schema`,"
            + "`g`.`OBJECT_NAME` AS `object_name`,"
            + "`g`.`OBJECT_TYPE` AS `object_type`,"
            + "`g`.`LOCK_TYPE` AS `lock_type`,"
            + "`g`.`LOCK_STATUS` AS `lock_status`,"
            + "`g`.`OWNER_THREAD_ID` AS `owner_thread_id` "
            + "from `performance_schema`.`metadata_locks` `g` ";

    private static final String SELECT_THREADS_ONLY =
        "select "
            + "`t`.`THREAD_ID` AS `thread_id`,"
            + "`t`.`PROCESSLIST_ID` AS `processlist_id`,"
            + "`t`.`PROCESSLIST_INFO` AS `processlist_info`,"
            + "`t`.`PROCESSLIST_TIME` AS `processlist_time` "
            + "from `performance_schema`.`threads` `t` ";
    /**
     * DDL will wait for MDL for some seconds, then kill the query which held the MDL
     */
    private static AtomicLong MDL_WAIT_TIMEOUT = new AtomicLong(15);

    static {
        // 只有server支持，这里是暂时改法，后续要将这段逻辑解耦
        try {
            killSyncActionClass =
                Class.forName("com.alibaba.polardbx.server.response.KillSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public MdlDeadlockDetectionTask(String db, Collection<String> allSchema, TransactionExecutor executor,
                                    int mdlWaitTimeoutInSec) {
        this.db = db;
        this.executor = executor;
        this.allSchema = allSchema;
        MDL_WAIT_TIMEOUT.set(mdlWaitTimeoutInSec);
    }

    private static class DbTableName {
        String dbName;
        String tableName;
        String objectType;

        static String phyTableNameFormat = "%s.%s";

        public DbTableName(String dbName, String tableName, String objectType) {
            this.dbName = dbName;
            this.tableName = tableName;
            this.objectType = objectType;
        }

        public Boolean isTable() {
            return objectType.equalsIgnoreCase("TABLE");
        }

        public String getPhyTableName() {
            return String.format(phyTableNameFormat, SqlIdentifier.surroundWithBacktick(dbName),
                SqlIdentifier.surroundWithBacktick(tableName));
        }

    }

    private static class MdlLockInfo {
        String lockStatus;
        String lockType;
        Long threadId;

        public MdlLockInfo(String lockStatus, String lockType, Long threadId) {
            this.lockStatus = lockStatus;
            this.lockType = lockType;
            this.threadId = threadId;
        }
    }

    private static class ProcessInfo {
        Long processId;
        Object processInfo;
        Long runningTime;

        public ProcessInfo(Long processId, Object processInfo, Long runningTime) {
            this.processId = processId;
            this.processInfo = processInfo;
            this.runningTime = runningTime;
        }
    }

    private static class MdlLockInfoSet {
        Map<String, List<MdlLockInfo>> mdlLockInfoMap;
        Boolean markForMdlWaiting;

        MdlLockInfoSet() {
            this.mdlLockInfoMap = new HashMap<>();
            this.markForMdlWaiting = false;
        }

        public void append(MdlLockInfo mdlLockInfo) {
            if (!this.mdlLockInfoMap.containsKey(mdlLockInfo.lockType)) {
                this.mdlLockInfoMap.put(mdlLockInfo.lockType, new ArrayList<>());
            }
            this.mdlLockInfoMap.get(mdlLockInfo.lockType).add(mdlLockInfo);
            if (this.markForMdlWaiting == false && DDL_WAIT_MDL_LOCK_TYPE.contains(mdlLockInfo.lockType)
                && mdlLockInfo.lockStatus.equalsIgnoreCase(PENDING_STATUS)) {
                this.markForMdlWaiting = true;
            }
        }

        public List<MdlWaitInfo> fetchWaitForInfoList(DbTableName dbTableName) {
            List<MdlWaitInfo> waitInfoList = new ArrayList<>();
            MdlLockInfo waitingMdlLockInfo = null;
            for (String lockType : this.mdlLockInfoMap.keySet()) {
                if (DDL_WAIT_MDL_LOCK_TYPE.contains(lockType)) {
                    if (waitingMdlLockInfo == null) {
                        List<MdlLockInfo> mdlLockInfos = this.mdlLockInfoMap.get(lockType);
                        for (MdlLockInfo mdlLockInfo : mdlLockInfos) {
                            if (mdlLockInfo.lockStatus.equalsIgnoreCase(PENDING_STATUS)) {
                                waitingMdlLockInfo = mdlLockInfo;
                            }
                        }
                    }
                }
            }
            for (String lockType : this.mdlLockInfoMap.keySet()) {
                if (waitingMdlLockInfo != null && !DDL_WAIT_MDL_LOCK_TYPE.contains(lockType)) {
                    List<MdlLockInfo> mdlLockInfos = this.mdlLockInfoMap.get(lockType);
                    for (MdlLockInfo mdlLockInfo : mdlLockInfos) {
                        if (mdlLockInfo.lockStatus.equalsIgnoreCase(GRANTED_STATUS)
                            && !waitingMdlLockInfo.threadId.equals(mdlLockInfo.threadId)) {
                            MdlWaitInfo mdlWaitInfo = new MdlWaitInfo(
                                waitingMdlLockInfo.threadId,
                                mdlLockInfo.threadId,
                                waitingMdlLockInfo.lockType,
                                mdlLockInfo.lockType,
                                -1L,
                                "",
                                "",
                                dbTableName.dbName,
                                dbTableName.tableName,
                                null);
                            waitInfoList.add(mdlWaitInfo);
                        }
                    }
                }
            }
            return waitInfoList;
        }

        public Boolean getMarkForMdlWaiting() {
            return this.markForMdlWaiting;
        }
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
    public static void fetchMetaDataLockWaits(String dn, TGroupDataSource dataSource,
                                              Collection<String> groupNames,
                                              DiGraph<TrxLookupSet.Transaction> graph,
                                              TrxLookupSet lookupSet,
                                              Map<Long, TGroupDataSource> mysqlConn2DatasourceMap,
                                              List<MdlWaitInfo> mdlWaitInfoList) {
        Long startMoment = System.currentTimeMillis();
        Long fetchMdlMoment = startMoment;
        Long processMdlMoment = startMoment;
        Long fetchProcessMoment = startMoment;
        Long processProcessMoment = startMoment;
        Long finalMoment = startMoment;
        int mdlCount = 0;
        int processCount = 0;
        final Map<Long, TrxLookupSet.Transaction> ddlTrxMap = new HashMap<>();
        final Set<String> phyDbNames =
            groupNames.stream().map(o -> buildPhysicalDbNameFromGroupName(o).toUpperCase()).collect(
                Collectors.toSet());
        String masterDnId = dataSource.getMasterDNId();
        List<MdlWaitInfo> mdlWaitInfoOnDataSource = new ArrayList<>();
        try (final Connection conn = DeadlockDetectionTask.createPhysicalConnectionForLeaderStorage(dataSource);
            Statement stmt = conn.createStatement()) {
//            final ResultSet rs = stmt.executeQuery(SELECT_PERFORMANCE_SCHEMA_METALOCKS);
            Map<String, MdlLockInfoSet> objectMdlMap = new HashMap<>();
            Boolean markForFetchThreadInfo = false;
            Map<String, DbTableName> dbTableNameMap = new HashMap<>();
            try (final ResultSet rs = stmt.executeQuery(SELECT_MDL_WAITING_ONLY)) {
                while (rs.next()) {
                    mdlCount++;
                    final String objectSchema = rs.getString("object_schema");
                    final String objectName = rs.getString("object_name");
                    final String objectType = rs.getString("object_type");
                    final String lockStatus = rs.getString("lock_status");
                    final String lockType = rs.getString("lock_type");
                    final Long threadId = rs.getLong("owner_thread_id");
                    DbTableName dbTableName = new DbTableName(objectSchema, objectName, objectType);
                    if (!dbTableName.isTable()) {
                        continue;
                    }
                    String phyTableName = dbTableName.getPhyTableName();
                    if (!objectMdlMap.containsKey(phyTableName)) {
                        objectMdlMap.put(phyTableName, new MdlLockInfoSet());
                        dbTableNameMap.put(phyTableName, dbTableName);
                    }
                    objectMdlMap.get(phyTableName).append(new MdlLockInfo(lockStatus, lockType, threadId));
                }
            }
            fetchMdlMoment = System.currentTimeMillis();
            for (String phyTableName : objectMdlMap.keySet()) {
                MdlLockInfoSet mdlLockInfoSet = objectMdlMap.get(phyTableName);
                if (mdlLockInfoSet.getMarkForMdlWaiting()) {
                    // short-way for non-ddl-waiting occasion.
                    markForFetchThreadInfo = true;
                    DbTableName dbTableName = dbTableNameMap.get(phyTableName);
                    mdlWaitInfoOnDataSource.addAll(mdlLockInfoSet.fetchWaitForInfoList(dbTableName));
                }
            }

            processMdlMoment = System.currentTimeMillis();
            fetchProcessMoment = processMdlMoment;
            if (markForFetchThreadInfo) {
                Map<Long, ProcessInfo> processInfoMap = new HashMap<Long, ProcessInfo>();
                try (final ResultSet rs2 = stmt.executeQuery(SELECT_THREADS_ONLY)) {
                    while (rs2.next()) {
                        processCount++;
                        final Long threadId = rs2.getLong("thread_id");
                        final Long processId = rs2.getLong("processlist_id");
                        // getString processlist_info is very costly
                        final Object processInfo = rs2.getObject("processlist_info");
                        final Long processTime = rs2.getLong("processlist_time");
                        processInfoMap.put(threadId, new ProcessInfo(processId, processInfo, processTime));
                    }
                }
                fetchProcessMoment = System.currentTimeMillis();
                for (MdlWaitInfo mdlWaitInfo : mdlWaitInfoOnDataSource) {
                    // update thread id => process id, and update process info, and more.
                    mdlWaitInfo.updateProcessInfo(processInfoMap.get(mdlWaitInfo.waiting),
                        processInfoMap.get(mdlWaitInfo.blocking), dataSource);
                }
            }

            processProcessMoment = System.currentTimeMillis();

            for (MdlWaitInfo mdlWaitInfo : mdlWaitInfoOnDataSource) {
                // Get the waiting and blocking processlist id
                final long waiting = mdlWaitInfo.waiting;
                final long blocking = mdlWaitInfo.blocking;

                // Update mysqlConn2DatasourceMap
                mysqlConn2DatasourceMap.put(waiting, dataSource);
                mysqlConn2DatasourceMap.put(blocking, dataSource);

                final Object waitingQuery = mdlWaitInfo.waitingQuery;
                final long waitingQuerySecs = mdlWaitInfo.waitingQuerySecs;
                final String waitingLockType = mdlWaitInfo.waitingLockType;
                final String blockingLockType = mdlWaitInfo.blockingLockType;
                final String physicalDbName = mdlWaitInfo.phyDbName;
                final Object blockingQuery = mdlWaitInfo.blockingQuery;
                final String physicalTableName = mdlWaitInfo.phyTableName;

                // Update mdlWaitInfoList, it is used to do the following thing:
                // If A is waiting for EXCLUSIVE MDL blocked by B, and B is holding a non-EXCLUSIVE MDL lock
                // Then we kill B
                if (waitingQuerySecs >= MDL_WAIT_TIMEOUT.get() &&
                    DDL_WAIT_MDL_LOCK_TYPE.contains(waitingLockType.toUpperCase()) &&
                    !DDL_WAIT_MDL_LOCK_TYPE.contains(blockingLockType.toUpperCase()) &&
                    phyDbNames.contains(physicalDbName.toUpperCase())) {
                    mdlWaitInfoList.add(mdlWaitInfo);
                }

                // The wait-block relation is not so accurate.
                // If A is waiting B, and B is waiting C, then we would detect A is waiting C.
                // In such case, if we put A->C in the graph, B may not appear in the final detected cycle.
                if (notBlockMdlType(waitingLockType, blockingLockType)) {
                    continue;
                }

                // Get the waiting and blocking transaction
                final Triple<TrxLookupSet.Transaction, TrxLookupSet.Transaction, String> waitingAndBlockingTrx =
                    lookupSet.getWaitingAndBlockingTrx(
                        Collections.singletonList(buildGroupNameFromPhysicalDb(physicalDbName)), waiting, blocking);

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
                    final Object truncatedSql = waitingQuery;
                    waitingLocalTrx.setPhysicalSql(truncatedSql);
                }

                final String physicalTable = String.format("`%s`.`%s`",
                    mdlWaitInfo.phyDbName, mdlWaitInfo.phyTableName);
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
                    final long blockingQuerySecs = mdlWaitInfo.waitingQuerySecs;
                    final Long startTime = System.currentTimeMillis() - blockingQuerySecs * 1000;
                    blockingTrx.setStartTime(startTime);
                } else {
                    final String groupName = waitingAndBlockingTrx.getRight();
                    blockingLocalTrx = blockingTrx.getLocalTransaction(groupName);
                }
                // Update other information of blocking trx
                if (null != blockingQuery && null == blockingLocalTrx.getPhysicalSql()) {
                    final Object truncatedSql = blockingQuery;
                    blockingLocalTrx.setPhysicalSql(truncatedSql);
                }

                // A lock id represents a unique lock,
                // and we use {schema.table:lock type} to represent an MDL lock id
                final String lockId = String.format("%s:%s", physicalTable, blockingLockType);
                blockingLocalTrx.addHoldingTrxLock(new TrxLock(lockId, blockingLockType, physicalTable));
                graph.addDiEdge(waitingTrx, blockingTrx);
            }
            finalMoment = System.currentTimeMillis();
            if (mdlWaitInfoList.size() > 0 || mdlCount > 100 || processCount > 100 || finalMoment - startMoment > 10) {
                String logInfo = String.format(
                    "mdl detection task fetch mdl cost total %d ms on dn %s(%s), fetch %d mdl, fetch %d process, generate %d mdl wait info: -----with %d ms on fetch mdl, %d ms on process mdl, %d ms on fetch process, %d ms on process process, %d ms on deadlock detect",
                    finalMoment - startMoment, dataSource.getMasterDNId(), dn, mdlCount, processCount,
                    mdlWaitInfoList.size(),
                    fetchMdlMoment - startMoment, processMdlMoment - fetchMdlMoment,
                    fetchProcessMoment - processMdlMoment,
                    processProcessMoment - fetchProcessMoment,
                    finalMoment - processProcessMoment);
                SQLRecorderLogger.ddlLogger.warn(logInfo);
            }
        } catch (SQLException ex) {
            final String dnId = dataSource.getMasterSourceAddress();
            final String logInfo =
                String.format("Failed to fetch lock waits on data source %s. msg:%s", dnId,
                    Optional.of(ex.getMessage()).orElse(" unknown cause"));
            SQLRecorderLogger.ddlLogger.error(logInfo);
            throw new RuntimeException(logInfo, ex);
        }
    }

    /**
     * @return Whether the blocking lock should block the waiting lock.
     * For example, SHARED_WRITE should NOT block SHARED_WRITE.
     */
    private static boolean notBlockMdlType(String waitingLockType, String blockingLockType) {
        final Set<String> notBlockMdlType = ImmutableSet.of("SHARED_WRITE", "SHARED_READ");
        return notBlockMdlType.contains(waitingLockType) && notBlockMdlType.contains(blockingLockType);
    }

    private void killByFrontendConnId(long frontendConnId) {
        ISyncAction killSyncAction;
        try {
            killSyncAction =
                (ISyncAction) killSyncActionClass
                    .getConstructor(String.class, Long.TYPE, Boolean.TYPE, Boolean.TYPE, ErrorCode.class)
                    .newInstance("", frontendConnId, false, true, ErrorCode.ERR_TRANS_DEADLOCK);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
        SyncManagerHelper.sync(killSyncAction, DEFAULT_DB_NAME, SyncScope.NOT_COLUMNAR_SLAVE);
    }

    private void killByBackendConnId(final Long backendConnId, final TGroupDataSource dataSource) {
        if (backendConnId == null || null == dataSource) {
            return;
        }
        String masterDnId = dataSource.getMasterDNId();
        try (Connection conn = DeadlockDetectionTask.createPhysicalConnectionForLeaderStorage(dataSource);
            Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(String.format("kill %s", backendConnId));
        } catch (SQLException ex) {
            final String dnId = dataSource.getMasterSourceAddress();
            throw new RuntimeException("Failed to kill connection on datasource " + dnId, ex);
        }
    }

    private void killByBackendConnIds(final List<Long> backendConnIds, final TGroupDataSource dataSource) {
        if (backendConnIds == null || null == dataSource) {
            return;
        }
        String masterDnId = dataSource.getMasterDNId();
        try (Connection conn = DeadlockDetectionTask.createPhysicalConnectionForLeaderStorage(dataSource);
            Statement stmt = conn.createStatement()) {
            List<String> sqls = new ArrayList<>();
            for (Long backendConnId : backendConnIds) {
                sqls.add(String.format("kill %s", backendConnId));
            }
            stmt.executeUpdate(StringUtils.join(sqls, ";"));
        } catch (SQLException ex) {
            final String dnId = dataSource.getMasterSourceAddress();
            throw new RuntimeException("Failed to kill connection on datasource " + dnId, ex);
        }
    }

    @Override
    public void run() {

        MDC.put(MDC.MDC_KEY_APP, DEFAULT_DB_NAME);
        if (!ConfigDataMode.isPolarDbX()) {
            cancel();
        }

        boolean hasLeadership = ExecUtils.hasLeadership(db);

        if (!hasLeadership) {
            TransactionLogger.debug("Skip MDL deadlock detection task since I am not the leader");
            return;
        }

        if (!runningCountByDb.containsKey(DEFAULT_DB_NAME)) {
            runningCountByDb.put(DEFAULT_DB_NAME, new AtomicLong(0));
        }

        int phyiscalMdlWaitTimeout = DynamicConfig.getInstance().getPhysicalMdlWaitTimeout();
        if (phyiscalMdlWaitTimeout <= 0) {
            if (runningCountByDb.get(DEFAULT_DB_NAME).get() > 0L) {
                EventLogger.log(EventType.DEAD_LOCK_DETECTION,
                    String.format("mdl detection has been shutdown for db %s!", DEFAULT_DB_NAME));
                runningCountByDb.get(DEFAULT_DB_NAME).set(-1L);
            }
            TransactionLogger.debug("Skip MDL deadlock detection task since dn mdl wait timeout is negative or zero");
            return;
        } else {
            MDL_WAIT_TIMEOUT.set(Math.max(phyiscalMdlWaitTimeout, MIN_MDL_WAIT_TIMEOUT_SECOND));
            long runningCount = runningCountByDb.get(DEFAULT_DB_NAME).get();
            if (runningCount < 0L) {
                EventLogger.log(EventType.DEAD_LOCK_DETECTION,
                    String.format("mdl detection has been launched for db %s, wait timeout %s s!", DEFAULT_DB_NAME,
                        MDL_WAIT_TIMEOUT));
                runningCountByDb.get(DEFAULT_DB_NAME).set(1L);
            } else {
                if (runningCount % 100L == 0) {
                    EventLogger.log(EventType.DEAD_LOCK_DETECTION,
                        String.format("mdl detection has been running for db %s, running count %d, wait timeout %s s!",
                            DEFAULT_DB_NAME, runningCount, MDL_WAIT_TIMEOUT));
                }
                runningCountByDb.get(DEFAULT_DB_NAME).incrementAndGet();
            }
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
        EventLogger.log(EventType.DEAD_LOCK_DETECTION, "the deadlock detection task is running on db " + db);
        final DiGraph<TrxLookupSet.Transaction> graph = new DiGraph<>();

        // Get all global transaction information
        final TrxLookupSet lookupSet = DeadlockDetectionTask.fetchTransInfo();

        // Get all group data sources, and group by DN's ID (host:port)
        final Map<String, List<TGroupDataSource>> instId2GroupList = ExecUtils.getInstId2GroupList(allSchema);

        final Map<Long, TGroupDataSource> mysqlConn2DatasourceMap = new HashMap<>();
        List<MdlWaitInfo> mdlWaitInfoList = new ArrayList<>();

        // For each DN, find the lock-wait information and add it to the graph
        for (String dn : instId2GroupList.keySet()) {
            final List<TGroupDataSource> groupDataSources = instId2GroupList.get(dn);
            if (CollectionUtils.isNotEmpty(groupDataSources)) {
                // Since all data sources are in the same DN, any data source is ok
                final TGroupDataSource groupDataSource = groupDataSources.get(0);

                // Get all group names in this DN
                final List<String> groupNames =
                    groupDataSources.stream().map(TGroupDataSource::getDbGroupKey).collect(Collectors.toList());

                // Fetch MDL-lock-wait information for this DN,
                // and update the graph, the lookup set, the mysqlConn2GroupMap, and the mdlWaitInfoList
                fetchMetaDataLockWaits(dn, groupDataSource, groupNames,
                    graph, lookupSet, mysqlConn2DatasourceMap, mdlWaitInfoList);
            }
        }
        graph.detect().ifPresent((cycle) -> {
            assert cycle.size() >= 2;
            final Pair<StringBuilder, StringBuilder> deadlockLog = DeadlockParser.parseMdlDeadlock(cycle);
            final StringBuilder simpleDeadlockLog = deadlockLog.getKey();
            final StringBuilder fullDeadlockLog = deadlockLog.getValue();

            Optional.ofNullable(OptimizerContext.getTransStat(DEFAULT_DB_NAME))
                .ifPresent(s -> s.countMdlDeadlock.incrementAndGet());

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

            // Record deadlock in meta db.
            try (Connection connection = MetaDbUtil.getConnection()) {
                DeadlocksAccessor deadlocksAccessor = new DeadlocksAccessor();
                deadlocksAccessor.setConnection(connection);
                deadlocksAccessor.recordDeadlock(GlobalTxLogManager.getCurrentServerAddr(), "MDL",
                    fullDeadlockLog.toString());
            } catch (Exception e) {
                logger.error(e);
            }
        });

        if (CollectionUtils.isNotEmpty(mdlWaitInfoList)) {
            Map<GroupConnPair, Long> groupConnPairLongMap = lookupSet.getGroupConn2Tran();
            Set<Long> logicalTransIds = new HashSet<>();
            for (MdlWaitInfo mdlWaitInfo : mdlWaitInfoList) {
                final String dnId = mdlWaitInfo.dataSource.getMasterSourceAddress();
                final String formatWaitingQuery = formatProcessInfo(mdlWaitInfo.waitingQuery);
                final String formatBlockingQuery = formatProcessInfo(mdlWaitInfo.blockingQuery);
                String groupName = buildGroupNameFromPhysicalDb(mdlWaitInfo.phyDbName);
                GroupConnPair groupConnPair =
                    new GroupConnPair(groupName, mdlWaitInfo.blocking);
                if (groupConnPairLongMap.containsKey(groupConnPair)) {
                    Long logicalTransId = groupConnPairLongMap.get(groupConnPair);
                    Long frontConnId = lookupSet.getTransaction(logicalTransId).getFrontendConnId();
                    if (!logicalTransIds.contains(frontConnId)) {
                        logicalTransIds.add(frontConnId);
                        final String killLogicalTransMessage = String.format(
                            "schema [%s] metadata lock wait time out: DN(%s) connection %s(%s) is trying to acquire %s MDL lock on %s.%s, "
                                + "but blocked by connection %s(%s) from CN connection %s. try kill CN connection %s with transId %s",
                            db, dnId, mdlWaitInfo.waiting, formatWaitingQuery, mdlWaitInfo.waitingLockType,
                            mdlWaitInfo.phyDbName, mdlWaitInfo.phyTableName, mdlWaitInfo.blocking,
                            formatBlockingQuery, frontConnId, frontConnId, logicalTransId);
                        logger.warn(killLogicalTransMessage);
                        killByFrontendConnId(frontConnId);
                        EventLogger.log(EventType.DEAD_LOCK_DETECTION, killLogicalTransMessage);
                    }
                } else {
                    // only kill physical connection when there is no corresponding logical connection
                    final String killMessage = String.format(
                        "schema [%s] metadata lock wait time out: DN(%s) connection %s(%s) is trying to acquire %s MDL lock on %s.%s, "
                            + "but blocked by connection %s(%s). try kill %s",
                        db, dnId, mdlWaitInfo.waiting, formatWaitingQuery, mdlWaitInfo.waitingLockType,
                        mdlWaitInfo.phyDbName, mdlWaitInfo.phyTableName, mdlWaitInfo.blocking, formatBlockingQuery,
                        mdlWaitInfo.blocking);
                    logger.warn(killMessage);
                    EventLogger.log(EventType.DEAD_LOCK_DETECTION, killMessage);
                    killByBackendConnId(mdlWaitInfo.blocking, mdlWaitInfo.dataSource);
                }

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
        String blockingLockType;
        long waitingQuerySecs;
        Object waitingQuery;

        Object blockingQuery;
        TGroupDataSource dataSource;

        String phyDbName;

        String phyTableName;

        public MdlWaitInfo(final long waiting,
                           final long blocking,
                           final String waitingLockType,
                           final String blockingLockType,
                           final long waitingQuerySecs,
                           final String waitingQuery,
                           final String blockingQuery,
                           final String phyDbName,
                           final String phyTableName,
                           final TGroupDataSource dataSource) {
            this.waiting = waiting;
            this.blocking = blocking;
            this.waitingLockType = waitingLockType;
            this.blockingLockType = blockingLockType;
            this.waitingQuerySecs = waitingQuerySecs;
            this.waitingQuery = Optional.ofNullable(waitingQuery).orElse("");
            this.blockingQuery = Optional.ofNullable(blockingQuery).orElse("");
            this.phyDbName = phyDbName;
            this.phyTableName = phyTableName;
            this.dataSource = dataSource;
        }

        public void updateProcessInfo(ProcessInfo waitingProcessInfo, ProcessInfo blockingProcessInfo,
                                      TGroupDataSource dataSource) {
            if (waitingProcessInfo != null) {
                if (waitingProcessInfo.processInfo != null) {
                    this.waitingQuery = waitingProcessInfo.processInfo;
                }
                this.waitingQuerySecs = waitingProcessInfo.runningTime;
                this.waiting = waitingProcessInfo.processId;
            } else {
                this.waiting = -1L;
            }
            if (blockingProcessInfo != null) {
                if (blockingProcessInfo.processInfo != null) {
                    this.blockingQuery = blockingProcessInfo.processInfo;
                }
                this.blocking = blockingProcessInfo.processId;
            } else {
                this.blocking = -1L;
            }
            this.dataSource = dataSource;
        }
    }

    private static String formatProcessInfo(Object processInfo) {
        return Optional.ofNullable(processInfo).orElse("").toString().replace("\n", "\\n");
    }
}
