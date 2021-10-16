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
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaInnodbTrxHandler;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.transaction.TransactionLogger;
import com.alibaba.polardbx.transaction.sync.FetchTransForDeadlockDetectionSyncAction;
import com.alibaba.polardbx.transaction.utils.DiGraph;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Deadlock detection task.
 *
 * @author TennyZhuang
 */
public class DeadlockDetectionTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(DeadlockDetectionTask.class);

    private final String db;

    private final TransactionExecutor executor;

    private static Class killSyncActionClass;

    static {
        // 只有server支持
        try {
            killSyncActionClass =
                Class.forName("com.alibaba.polardbx.server.response.KillSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public DeadlockDetectionTask(String db, TransactionExecutor executor) {
        this.db = db;
        this.executor = executor;
    }

    public void fetchLockWaits(String group, IDataSource dataSource, BiConsumer<Long, Long> consumer) {
        try (IConnection conn = dataSource.getConnection(); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery(
                "SELECT trx_a.trx_mysql_thread_id AS waiting, trx_b.trx_mysql_thread_id AS blocking " +
                    "FROM information_schema.innodb_lock_waits, information_schema.INNODB_TRX AS trx_a, information_schema.INNODB_TRX AS trx_b "
                    +
                    "WHERE trx_a.trx_id = requesting_trx_id AND trx_b.trx_id = blocking_trx_id;");
            while (rs.next()) {
                final long waiting = rs.getLong(1);
                final long blocking = rs.getLong(2);
                consumer.accept(waiting, blocking);
            }
        } catch (SQLException ex) {
            throw new RuntimeException("Failed to fetch lock waits on group " + group, ex);
        }
    }

    public InformationSchemaInnodbTrxHandler.LookupSet fetchTransInfo(String db) {
        final HashMap<InformationSchemaInnodbTrxHandler.GroupConnPair, Long> connGroup2Tran = new HashMap<>();
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
                InformationSchemaInnodbTrxHandler.GroupConnPair
                    entry = new InformationSchemaInnodbTrxHandler.GroupConnPair(group, connId);
                connGroup2Tran.put(entry, transId);
                tran2FrontendConn.putIfAbsent(transId, frontendConnId);
            }
        }
        return new InformationSchemaInnodbTrxHandler.LookupSet(connGroup2Tran, tran2FrontendConn);
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

    @Override
    public void run() {

        boolean hasLeadership = ExecUtils.hasLeadership(db);

        if (!hasLeadership) {
            TransactionLogger.getLogger().debug("Skip deadlock detection task since I am not the leader");
            return;
        }
        logger.debug("Deadlock detection task starts.");
        try {
            final DiGraph<Long> graph = new DiGraph<>();
            final InformationSchemaInnodbTrxHandler.LookupSet lookupSet = fetchTransInfo(db);
            for (String group : executor.getGroupList()) {
                final IDataSource dataSource = executor.getGroupExecutor(group).getDataSource();
                fetchLockWaits(group, dataSource, (waiting, blocking) -> {
                    final Long waitingTrx = lookupSet.connGroup2Tran
                        .get(new InformationSchemaInnodbTrxHandler.GroupConnPair(group, waiting));
                    final Long blockingTrx = lookupSet.connGroup2Tran
                        .get(new InformationSchemaInnodbTrxHandler.GroupConnPair(group, blocking));
                    if (waitingTrx == null || blockingTrx == null) {
                        return;
                    }
                    graph.addDiEdge(waitingTrx, blockingTrx);
                });
            }
            graph.detect().ifPresent((cycle) -> {
                assert cycle.size() >= 2;
                StringBuilder sb = new StringBuilder();
                sb.append("Deadlock detected, ");
                for (int i = 0; i < cycle.size() - 1; i++) {
                    long u = cycle.get(i);
                    long v = cycle.get(i + 1);
                    sb.append(Long.toHexString(u)).append(" waiting ").append(Long.toHexString(v)).append(", ");
                }

                // TODO: kill transaction by some priority, such as create time, or prefer to kill internal transaction.
                Long toKill = cycle.get(0);
                sb.append("Will rollback ").append(Long.toHexString(toKill)).append('.');
                String errorDetail = sb.toString();
                TransactionLogger.getLogger().warn(errorDetail);
                logger.warn(errorDetail);
                EventLogger.log(EventType.DEAD_LOCK_DETECTION, errorDetail);

                long toKillFrontendConnId = lookupSet.tran2FrontendConnId.get(toKill);
                killByFrontendConnId(toKillFrontendConnId);
            });
        } catch (Throwable ex) {
            logger.error("Failed to do deadlock detection", ex);
        }
    }

}
