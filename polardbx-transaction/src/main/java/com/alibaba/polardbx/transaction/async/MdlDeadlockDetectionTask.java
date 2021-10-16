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
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.sync.FetchTransForDeadlockDetectionSyncAction;
import com.alibaba.polardbx.transaction.utils.DiGraph;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

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
            + "`sys`.`format_statement`(`pt`.`PROCESSLIST_INFO`) AS `waiting_query`,"
            + "`pt`.`PROCESSLIST_TIME` AS `waiting_query_secs`,"
            + "`ps`.`ROWS_AFFECTED` AS `waiting_query_rows_affected`,"
            + "`ps`.`ROWS_EXAMINED` AS `waiting_query_rows_examined`,"
            + "`gt`.`THREAD_ID` AS `blocking_thread_id`,"
            + "`gt`.`PROCESSLIST_ID` AS `blocking_pid`,"
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

    public List<MdlWaitInfo> fetchMetaDataLockWaits(String group, IDataSource dataSource,
                                                    BiConsumer<Long, Long> consumer) {
        List<MdlWaitInfo> mdlInfoList = new ArrayList<>();
        try (IConnection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(SELECT_PERFORMANCE_SCHEMA_METALOCKS);
            while (rs.next()) {
                final long waiting = rs.getLong("waiting_pid");
                final long blocking = rs.getLong("blocking_pid");
                consumer.accept(waiting, blocking);

                final String waitingLockType = rs.getString("waiting_lock_type");
                final String blocking_lock_type = rs.getString("blocking_lock_type");
                final long waitingQuerySecs = rs.getLong("waiting_query_secs");
                final String waitingQuery = rs.getString("waiting_query");

                //if A is waiting for EXCLUSIVE MDL blocked by B, and B is holding a non-EXCLUSIVE MDL lock
                //then we kill B
                if (waitingQuerySecs >= MDL_WAIT_TIMEOUT &&
                    StringUtils.equalsIgnoreCase(waitingLockType, "EXCLUSIVE")
                    && !StringUtils.equalsIgnoreCase(blocking_lock_type, "EXCLUSIVE")) {
                    MdlWaitInfo mdlInfo =
                        new MdlWaitInfo(waiting, blocking, waitingLockType, waitingQuerySecs, waitingQuery, group);
                    mdlInfoList.add(mdlInfo);
                }
            }
            return mdlInfoList;
        } catch (SQLException ex) {
            throw new RuntimeException(
                String.format("Failed to fetch lock waits on group %s. msg:%s", group, ex.getMessage()), ex);
        }
    }

    public LookupSet fetchTransInfo(String db) {
        final HashMap<GroupConnPair, Long> connGroup2Tran = new HashMap<>();
        final HashMap<Long, Long> tran2FrontendConn = new HashMap<>();
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
                GroupConnPair entry = new GroupConnPair(group, connId);
                connGroup2Tran.put(entry, transId);
                tran2FrontendConn.putIfAbsent(transId, frontendConnId);
            }
        }
        return new LookupSet(connGroup2Tran, tran2FrontendConn);
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

    private void killByBackendConnId(final Long backendConnId, final String group) {
        if (backendConnId == null || StringUtils.isEmpty(group)) {
            return;
        }
        final IDataSource dataSource = executor.getGroupExecutor(group).getDataSource();
        try (IConnection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(String.format("kill %s", backendConnId));
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to kill connection on group " + group, ex);
        }
    }

    @Override
    public void run() {

        boolean hasLeadership = ExecUtils.hasLeadership(db);

        if (!hasLeadership) {
            TransactionLogger.getLogger().debug("Skip MDL deadlock detection task since I am not the leader");
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
        final DiGraph<Long> graph = new DiGraph<>();
        final LookupSet lookupSet = fetchTransInfo(db);
        final Map<Long, String> mysqlConn2GroupMap = new HashMap<>();
        List<MdlWaitInfo> mdlWaitInfoList = new ArrayList<>();
        for (String group : executor.getGroupList()) {
            final IDataSource dataSource = executor.getGroupExecutor(group).getDataSource();
            mdlWaitInfoList = fetchMetaDataLockWaits(group, dataSource, (waiting, blocking) -> {
                mysqlConn2GroupMap.put(waiting, group);
                mysqlConn2GroupMap.put(blocking, group);

                Long waitingTrx = lookupSet.connGroup2Tran.get(new GroupConnPair(group, waiting));
                Long blockingTrx = lookupSet.connGroup2Tran.get(new GroupConnPair(group, blocking));

                if (waitingTrx == null) {
                    waitingTrx = waiting;
                }
                if (blockingTrx == null) {
                    blockingTrx = blocking;
                }

                graph.addDiEdge(waitingTrx, blockingTrx);
            });
        }
        graph.detect().ifPresent((cycle) -> {
            assert cycle.size() >= 2;
            StringBuilder sb = new StringBuilder();
            sb.append("MDL MetaData Deadlock detected at schema: " + db + ", ");
            for (int i = 0; i < cycle.size() - 1; i++) {
                long u = cycle.get(i);
                long v = cycle.get(i + 1);
                sb.append(Long.toHexString(u)).append(" waiting ").append(Long.toHexString(v)).append(", ");
            }

            ArrayList<Long> backendConnList = new ArrayList<>();
            ArrayList<Long> frontendConnList = new ArrayList<>();

            for (int i = 0; i < cycle.size(); i++) {
                Long toKill = cycle.get(i);
                Long toKillFrontendConnId = lookupSet.tran2FrontendConnId.get(toKill);
                if (toKillFrontendConnId == null) {
                    backendConnList.add(toKill);
                } else {
                    frontendConnList.add(toKillFrontendConnId);
                }
            }

            if (CollectionUtils.isNotEmpty(frontendConnList)) {
                Long victim = frontendConnList.get(0);
                sb.append("Will rollback ").append(Long.toHexString(victim)).append('.');
                String errorDetail = sb.toString();
                TransactionLogger.getLogger().warn(errorDetail);
                logger.warn(errorDetail);
                EventLogger.log(EventType.DEAD_LOCK_DETECTION, errorDetail);

                killByFrontendConnId(victim);
                return;
            }

            if (CollectionUtils.isNotEmpty(backendConnList)) {
                Long victim = backendConnList.get(0);
                sb.append("Will rollback ").append(Long.toHexString(victim)).append('.');
                String errorDetail = sb.toString();
                TransactionLogger.getLogger().warn(errorDetail);
                logger.warn(errorDetail);
                EventLogger.log(EventType.DEAD_LOCK_DETECTION, errorDetail);

                killByBackendConnId(victim, mysqlConn2GroupMap.get(victim));
                return;
            }
        });
        if (!graph.detect().isPresent() && CollectionUtils.isNotEmpty(mdlWaitInfoList)) {
            for (MdlWaitInfo mdlWaitInfo : mdlWaitInfoList) {
                final String killMessage = String.format(
                    "DN(group=%s) connection %s is trying to acquire EXCLUSIVE MDL lock, "
                        + "but blocked by connection %s. try kill %s",
                    mdlWaitInfo.group, mdlWaitInfo.waiting, mdlWaitInfo.blocking, mdlWaitInfo.blocking);
                logger.warn(killMessage);
                EventLogger.log(EventType.DEAD_LOCK_DETECTION, killMessage);
                killByBackendConnId(mdlWaitInfo.blocking, mdlWaitInfo.group);
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

    private static class GroupConnPair {
        final private String group;
        final private long connId;

        private GroupConnPair(String group, long connId) {
            this.group = group;
            this.connId = connId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            GroupConnPair that = (GroupConnPair) o;
            return connId == that.connId &&
                group.equals(that.group);
        }

        @Override
        public int hashCode() {
            return Objects.hash(group, connId);
        }
    }

    private static class LookupSet {
        HashMap<GroupConnPair, Long> connGroup2Tran;
        HashMap<Long, Long> tran2FrontendConnId;

        public LookupSet(HashMap<GroupConnPair, Long> connGroup2Tran,
                         HashMap<Long, Long> tran2FrontendConnId) {
            this.connGroup2Tran = connGroup2Tran;
            this.tran2FrontendConnId = tran2FrontendConnId;
        }
    }

    private static class MdlWaitInfo {
        long waiting;
        long blocking;
        String waitingLockType;
        long waitingQuerySecs;
        String waitingQuery;
        String group;

        public MdlWaitInfo(final long waiting,
                           final long blocking,
                           final String waitingLockType,
                           final long waitingQuerySecs,
                           final String waitingQuery,
                           final String group) {
            this.waiting = waiting;
            this.blocking = blocking;
            this.waitingLockType = waitingLockType;
            this.waitingQuerySecs = waitingQuerySecs;
            this.waitingQuery = waitingQuery;
            this.group = group;
        }
    }
}
