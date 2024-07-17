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

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.transaction.TransactionUtils;
import com.alibaba.polardbx.executor.utils.transaction.TrxLookupSet;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaInnodbTrx;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author dylan
 */
public class InformationSchemaInnodbTrxHandler extends BaseVirtualViewSubClassHandler {

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
        TrxLookupSet lookupSet = TransactionUtils.getTrxLookupSet(schemaNames);

        Map<String, List<TGroupDataSource>> instId2GroupList = ExecUtils.getInstId2GroupList(schemaNames);

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

                    Long tranId = lookupSet.getTransactionId(groupNameList, trx_mysql_thread_id);
                    if (tranId == null) {
                        continue;
                    }

                    String sql = lookupSet.getSql(tranId);

                    if (processedTranId.add(tranId)) {
                        cursor.addRow(new Object[] {
                            Long.toHexString(tranId),
                            trx_state,
                            lookupSet.getStartTime(tranId) != null ?
                                new Timestamp(lookupSet.getStartTime(tranId)) : trx_started,
                            trx_requested_lock_id,
                            trx_wait_started,
                            trx_weight,
                            lookupSet.getFrontendConnId(tranId),
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

}


