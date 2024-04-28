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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowTrans;

import java.util.List;
import java.util.Map;

public class ShowTransHandler extends HandlerCommon {

    private static final Class showTransSyncActionClass;

    static {
        try {
            showTransSyncActionClass = Class.forName("com.alibaba.polardbx.server.response.ShowTransSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public ShowTransHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowTrans showTrans = (SqlShowTrans) show.getNativeSqlNode();
        final boolean isColumnar = showTrans.isColumnar();

        ArrayResultCursor result = new ArrayResultCursor("TRANSACTIONS");
        result.addColumn("TRANS_ID", DataTypes.StringType);
        result.addColumn("TYPE", DataTypes.StringType);
        result.addColumn("DURATION_MS", DataTypes.LongType);
        result.addColumn("STATE", DataTypes.StringType);
        result.addColumn("PROCESS_ID", DataTypes.LongType);
        if (isColumnar) {
            result.addColumn("TSO", DataTypes.LongType);
        }

        ISyncAction syncAction;
        try {
            syncAction = (ISyncAction) showTransSyncActionClass.getConstructor(String.class, boolean.class)
                .newInstance(executionContext.getSchemaName(), isColumnar);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }

        List<List<Map<String, Object>>> results = SyncManagerHelper.sync(syncAction, executionContext.getSchemaName(),
            SyncScope.ALL);

        for (List<Map<String, Object>> rs : results) {
            if (rs == null) {
                continue;
            }
            for (Map<String, Object> row : rs) {
                final String transId = (String) row.get("TRANS_ID");
                final String type = (String) row.get("TYPE");
                final long duration = (Long) row.get("DURATION_MS");
                final String state = (String) row.get("STATE");
                final long processId = (Long) row.get("PROCESS_ID");

                if (isColumnar) {
                    final long tso = (Long) row.get("TSO");
                    result.addRow(new Object[] {transId, type, duration, state, processId, tso});
                } else {
                    result.addRow(new Object[] {transId, type, duration, state, processId});
                }
            }
        }

        return result;
    }
}
