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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaInnodbTrx;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author dylan
 */
public class InformationSchemaInnodbTrxHandler extends BaseVirtualViewSubClassHandler {

    private static Class fetchAllTransSyncActionClass;

    static {
        try {
            fetchAllTransSyncActionClass =
                Class.forName("com.alibaba.polardbx.transaction.sync.FetchAllTransSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public InformationSchemaInnodbTrxHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaInnodbTrx;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        Set<Long> processedTranId = new HashSet<>();
        Set<String> schemaNames = OptimizerContext.getActiveSchemaNames();
        TransInfo transInfo = new TransInfo(schemaNames);

        Map<String, List<TGroupDataSource>> instId2GroupList = virtualViewHandler.getInstId2GroupList(schemaNames);

        for (List<TGroupDataSource> groupDataSourceList : instId2GroupList.values()) {

            TGroupDataSource repGroupDataSource = groupDataSourceList.get(0);

            List<String> groupNameList =
                groupDataSourceList.stream().map(x -> x.getDbGroupKey()).collect(Collectors.toList());

            try (IConnection conn = repGroupDataSource.getConnection(); Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(
                    "select trx_id, trx_state, trx_started, trx_requested_lock_id, trx_wait_started, "
                        + "trx_weight, trx_mysql_thread_id, trx_query, trx_operation_state, trx_tables_in_use, "
                        + "trx_tables_locked, trx_lock_structs, trx_lock_memory_bytes, trx_rows_locked, "
                        + "trx_rows_modified, trx_concurrency_tickets, trx_isolation_level, trx_unique_checks, "
                        + "trx_foreign_key_checks, trx_last_foreign_key_error, trx_adaptive_hash_latched, "
                        + "trx_adaptive_hash_timeout, trx_is_read_only, trx_autocommit_non_locking "
                        + "from information_schema.innodb_trx");

                while (rs.next()) {

                    String trx_id = rs.getString("trx_id");
                    String trx_state = rs.getString("trx_state");
                    Timestamp trx_started = rs.getTimestamp("trx_started");
                    String trx_requested_lock_id = rs.getString("trx_requested_lock_id");
                    Timestamp trx_wait_started = rs.getTimestamp("trx_wait_started");
                    Long trx_weight = rs.getLong("trx_weight");
                    Long trx_mysql_thread_id = rs.getLong("trx_mysql_thread_id");
                    String trx_query = rs.getString("trx_query");
                    String trx_operation_state = rs.getString("trx_operation_state");
                    Long trx_tables_in_use = rs.getLong("trx_tables_in_use");
                    Long trx_tables_locked = rs.getLong("trx_tables_locked");
                    Long trx_lock_structs = rs.getLong("trx_lock_structs");
                    Long trx_lock_memory_bytes = rs.getLong("trx_lock_memory_bytes");
                    Long trx_rows_locked = rs.getLong("trx_rows_locked");
                    Long trx_rows_modified = rs.getLong("trx_rows_modified");
                    Long trx_concurrency_tickets = rs.getLong("trx_concurrency_tickets");
                    String trx_isolation_level = rs.getString("trx_isolation_level");
                    Long trx_unique_checks = rs.getLong("trx_unique_checks");
                    Long trx_foreign_key_checks = rs.getLong("trx_foreign_key_checks");
                    String trx_last_foreign_key_error = rs.getString("trx_last_foreign_key_error");
                    Long trx_adaptive_hash_latched = rs.getLong("trx_adaptive_hash_latched");
                    Long trx_adaptive_hash_timeout = rs.getLong("trx_adaptive_hash_timeout");
                    Long trx_is_read_only = rs.getLong("trx_is_read_only");
                    Long trx_autocommit_non_locking = rs.getLong("trx_autocommit_non_locking");

                    Long tranId = transInfo.mysqlConnId2PolarDbXTranId(groupNameList, trx_mysql_thread_id);
                    if (tranId == null) {
                        continue;
                    }

                    String sql = transInfo.tranId2PolarDbXSql(tranId);

                    if (processedTranId.add(tranId)) {
                        cursor.addRow(new Object[] {
                            Long.toHexString(tranId),
                            trx_state,
                            transInfo.tranId2StartTime(tranId) != null ?
                                new Timestamp(transInfo.tranId2StartTime(tranId)) : trx_started,
                            trx_requested_lock_id,
                            trx_wait_started,
                            trx_weight,
                            transInfo.tranId2PolarDbXFrontendConnId(tranId),
                            trx_query != null ? sql : null,
                            trx_operation_state,
                            trx_tables_in_use,
                            trx_tables_locked,
                            trx_lock_structs,
                            trx_lock_memory_bytes,
                            trx_rows_locked,
                            trx_rows_modified,
                            trx_concurrency_tickets,
                            trx_isolation_level,
                            trx_unique_checks,
                            trx_foreign_key_checks,
                            trx_last_foreign_key_error,
                            trx_adaptive_hash_latched,
                            trx_adaptive_hash_timeout,
                            trx_is_read_only,
                            trx_autocommit_non_locking
                        });
                    }
                }
            } catch (SQLException ex) {
                throw new RuntimeException(
                    "Failed to fetch innodb_locks on group " + repGroupDataSource.getDbGroupKey(), ex);
            }

        }

        return cursor;
    }

    public static class TransInfo {

        LookupSet lookupSet;

        HashMap<Long, String> tran2Sql = new HashMap<>();

        HashMap<Long, Long> tran2StartTime = new HashMap<>();

        public TransInfo(Collection<String> schemaNames) {
            if (schemaNames == null || schemaNames.isEmpty()) {
                return;
            }

            HashMap<GroupConnPair, Long> connGroup2Tran = new HashMap<>();
            HashMap<Long, Long> tran2FrontendConnId = new HashMap<>();

            for (String schemaName : schemaNames) {
                ISyncAction fetchAllTransSyncAction;
                try {
                    fetchAllTransSyncAction =
                        (ISyncAction) fetchAllTransSyncActionClass.getConstructor(String.class, boolean.class)
                            .newInstance(schemaName, true);
                } catch (Exception e) {
                    throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
                }

                final List<List<Map<String, Object>>> results =
                    SyncManagerHelper.sync(fetchAllTransSyncAction, schemaName);

                for (List<Map<String, Object>> result : results) {
                    if (result == null) {
                        continue;
                    }
                    for (Map<String, Object> row : result) {
                        final Long transId = (Long) row.get("TRANS_ID");
                        final String group = (String) row.get("GROUP");
                        final Long connId = (Long) row.get("CONN_ID");
                        final Long frontendConnId = (Long) row.get("FRONTEND_CONN_ID");
                        final Long startTime = (Long) row.get("START_TIME");
                        final String sql = (String) row.get("SQL");

                        GroupConnPair groupConnPair = new GroupConnPair(group, connId);
                        connGroup2Tran.put(groupConnPair, transId);
                        tran2FrontendConnId.put(transId, frontendConnId);
                        tran2Sql.put(transId, sql);
                        tran2StartTime.put(transId, startTime);
                    }
                }
            }

            lookupSet = new LookupSet(connGroup2Tran, tran2FrontendConnId);
        }

        public Long mysqlConnId2PolarDbXTranId(String group, Long connId) {
            return lookupSet.connGroup2Tran.get(new GroupConnPair(group, connId));
        }

        public Long tranId2PolarDbXFrontendConnId(Long tranId) {
            return lookupSet.tran2FrontendConnId.get(tranId);
        }

        public Long mysqlConnId2PolarDbXTranId(Collection<String> groupNameList, Long connId) {
            Long tranId = null;
            for (String groupName : groupNameList) {
                tranId = this.mysqlConnId2PolarDbXTranId(groupName, connId);
                if (tranId != null) {
                    break;
                }
            }
            return tranId;
        }

        public String tranId2PolarDbXSql(Long tranId) {
            return tran2Sql.get(tranId);
        }

        public Long tranId2StartTime(Long tranId) {
            return tran2StartTime.get(tranId);
        }

        public Set<Long> allTranId() {
            return lookupSet.tran2FrontendConnId.keySet();
        }
    }

    public static class GroupConnPair {
        final private String group;
        final private long connId;

        public GroupConnPair(String group, long connId) {
            this.group = group;
            this.connId = connId;
        }

        public String getGroup() {
            return group;
        }

        public long getConnId() {
            return connId;
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

    public static class LookupSet {
        public HashMap<GroupConnPair, Long> connGroup2Tran;
        public HashMap<Long, Long> tran2FrontendConnId;

        public LookupSet(HashMap<GroupConnPair, Long> connGroup2Tran,
                         HashMap<Long, Long> tran2FrontendConnId) {
            this.connGroup2Tran = connGroup2Tran;
            this.tran2FrontendConnId = tran2FrontendConnId;
        }
    }
}


