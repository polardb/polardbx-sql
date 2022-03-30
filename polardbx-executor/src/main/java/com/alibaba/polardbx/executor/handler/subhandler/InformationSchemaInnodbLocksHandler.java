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

import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.transaction.TransactionUtils;
import com.alibaba.polardbx.executor.utils.transaction.TrxLookupSet;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaInnodbLocks;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author dylan
 */
public class InformationSchemaInnodbLocksHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaInnodbLocksHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaInnodbLocks;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        Set<String> schemaNames = OptimizerContext.getActiveSchemaNames();
        TrxLookupSet lookupSet = TransactionUtils.getTrxLookupSet(schemaNames);
        Map<String, List<TGroupDataSource>> instId2GroupList = ExecUtils.getInstId2GroupList(schemaNames);

        for (List<TGroupDataSource> groupDataSourceList : instId2GroupList.values()) {

            TGroupDataSource repGroupDataSource = groupDataSourceList.get(0);

            List<String> groupNameList =
                groupDataSourceList.stream().map(x -> x.getDbGroupKey()).collect(Collectors.toList());

            try (IConnection conn = repGroupDataSource.getConnection(); Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(
                    "SELECT lock_id, lock_trx_id, lock_mode, lock_type, lock_table, lock_index, lock_space, "
                        + "lock_page, lock_rec, lock_data, trx_mysql_thread_id "
                        + "FROM information_schema.INNODB_LOCKS join information_schema.INNODB_TRX "
                        + "on lock_trx_id = trx_id");

                while (rs.next()) {
                    String lock_id = rs.getString("lock_id");
                    String lock_trx_id = rs.getString("lock_trx_id");
                    String lock_mode = rs.getString("lock_mode");
                    String lock_type = rs.getString("lock_type");
                    String lock_table = rs.getString("lock_table");
                    String lock_index = rs.getString("lock_index");
                    long lock_space = rs.getLong("lock_space");
                    long lock_page = rs.getLong("lock_page");
                    long lock_rec = rs.getLong("lock_rec");
                    String lock_data = rs.getString("lock_data");
                    long trx_mysql_thread_id = rs.getLong("trx_mysql_thread_id");

                    Long tranId = lookupSet.getTransactionId(groupNameList, trx_mysql_thread_id);

                    if (tranId == null) {
                        continue;
                    }

                    cursor.addRow(new Object[] {
                        lock_id,
                        Long.toHexString(tranId),
                        lock_mode,
                        lock_type,
                        lock_table,
                        lock_index,
                        lock_space,
                        lock_page,
                        lock_rec,
                        lock_data
                    });
                }
            } catch (SQLException ex) {
                throw new RuntimeException(
                    "Failed to fetch innodb_locks on group " + repGroupDataSource.getDbGroupKey(), ex);
            }
        }
        return cursor;
    }
}

