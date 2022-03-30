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
import com.alibaba.polardbx.optimizer.view.InformationSchemaInnodbLockWaits;
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
public class InformationSchemaInnodbLockWaitsHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaInnodbLockWaitsHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaInnodbLockWaits;
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
                    "select requesting_trx_id,requested_lock_id,blocking_trx_id,blocking_lock_id,"
                        + "a.trx_mysql_thread_id as requesting_mysql_thread_id, "
                        + "b.trx_mysql_thread_id as blocking_mysql_thread_id "
                        + "from information_schema.innodb_lock_waits "
                        + "join information_schema.innodb_trx a on a.trx_id = requesting_trx_id "
                        + "join information_schema.innodb_trx b on b.trx_id = blocking_trx_id ");

                while (rs.next()) {
                    String requesting_trx_id = rs.getString("requesting_trx_id");
                    String requested_lock_id = rs.getString("requested_lock_id");
                    String blocking_trx_id = rs.getString("blocking_trx_id");
                    String blocking_lock_id = rs.getString("blocking_lock_id");
                    Long requesting_mysql_thread_id = rs.getLong("requesting_mysql_thread_id");
                    Long blocking_mysql_thread_id = rs.getLong("blocking_mysql_thread_id");

                    Long requestingTranId = lookupSet.getTransactionId(groupNameList, requesting_mysql_thread_id);
                    Long blockingTranId = lookupSet.getTransactionId(groupNameList, blocking_mysql_thread_id);
                    if (requestingTranId == null || blockingTranId == null) {
                        continue;
                    }

                    cursor.addRow(new Object[] {
                        Long.toHexString(requestingTranId),
                        requested_lock_id,
                        Long.toHexString(blockingTranId),
                        blocking_lock_id
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


