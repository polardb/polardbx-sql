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

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.privilege.PolarPrivUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.statis.SQLRecord;
import com.alibaba.polardbx.optimizer.statis.SQLRecorder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowSlow;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public class LogicalShowSlowHandler extends HandlerCommon {

    private static Class showSlowSyncActionClass;
    private static Class showPhysicalSlowActionClass;

    static {
        try {
            showSlowSyncActionClass = Class.forName("com.alibaba.polardbx.server.response.ShowSQLSlowSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }

        try {
            showPhysicalSlowActionClass =
                Class.forName("com.alibaba.polardbx.server.response.ShowPhysicalSQLSlowSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public LogicalShowSlowHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowSlow showSlow = (SqlShowSlow) show.getNativeSqlNode();

        ArrayResultCursor result = new ArrayResultCursor("SLOW");

        if (showPhysicalSlowActionClass == null
            || showSlowSyncActionClass == null) {
            throw new NotSupportException();
        }

        if (!showSlow.isPhysical()) {
            result.addColumn("TRACE_ID", DataTypes.StringType);
            result.addColumn("USER", DataTypes.StringType);
            result.addColumn("HOST", DataTypes.StringType);
            result.addColumn("DB", DataTypes.StringType);
            result.addColumn("START_TIME", DataTypes.DatetimeType);
            result.addColumn("EXECUTE_TIME", DataTypes.LongType);
            result.addColumn("AFFECT_ROW", DataTypes.LongType);
            result.addColumn("SQL", DataTypes.StringType);
            result.initMeta();

            ISyncAction showSlowAction;
            try {
                showSlowAction = (ISyncAction) showSlowSyncActionClass.getConstructor(String.class)
                    .newInstance(executionContext.getSchemaName());
            } catch (Exception e) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());

            }

            List<List<Map<String, Object>>> results =
                SyncManagerHelper.sync(showSlowAction, executionContext.getSchemaName());
            int size = 0;
            for (List<Map<String, Object>> rs : results) {
                if (rs == null) {
                    continue;
                }
                size += rs.size();
            }
            if (size == 0) {
                return result;
            }
            SQLRecorder recorder = new SQLRecorder(Math.min(size, 100));
            for (List<Map<String, Object>> rs : results) {
                if (rs == null) {
                    continue;
                }

                for (Map<String, Object> row : rs) {
                    SQLRecord record = new SQLRecord();
                    record.traceId = DataTypes.StringType.convertFrom(row.get("TRACE_ID"));
                    record.user = DataTypes.StringType.convertFrom(row.get("USER"));
                    record.host = DataTypes.StringType.convertFrom(row.get("HOST"));
                    record.port = DataTypes.StringType.convertFrom(row.get("PORT"));
                    record.schema = DataTypes.StringType.convertFrom(row.get("SCHEMA"));
                    record.startTime = DataTypes.LongType.convertFrom(row.get("START_TIME"));
                    record.executeTime = DataTypes.LongType.convertFrom(row.get("EXECUTE_TIME"));
                    record.affectRow = DataTypes.LongType.convertFrom(row.get("AFFECT_ROW"));
                    record.statement = DataTypes.StringType.convertFrom(row.get("SQL"));
                    recorder.add(record);
                }
            }

            SQLRecord[] records = recorder.getSortedRecords();
            for (int i = records.length - 1; i >= 0; i--) {
                if (records[i] != null) {
                    result.addRow(new Object[] {
                        records[i].traceId, records[i].user, records[i].host, records[i].schema,
                        new Date(records[i].startTime), records[i].executeTime, records[i].affectRow,
                        records[i].statement});
                }
            }
        } else {
            result.addColumn("TRACE_ID", DataTypes.StringType);
            result.addColumn("GROUP_NAME", DataTypes.StringType);
            result.addColumn("DBKEY_NAME", DataTypes.StringType);
            result.addColumn("START_TIME", DataTypes.DatetimeType);
            result.addColumn("EXECUTE_TIME", DataTypes.LongType);
            result.addColumn("SQL_EXECUTE_TIME", DataTypes.LongType);
            result.addColumn("GETLOCK_CONNECTION_TIME", DataTypes.LongType);
            result.addColumn("CREATE_CONNECTION_TIME", DataTypes.LongType);
            result.addColumn("AFFECT_ROW", DataTypes.LongType);
            result.addColumn("SQL", DataTypes.StringType);
            result.initMeta();

            ISyncAction showPhysicalSlowAction;
            try {
                showPhysicalSlowAction = (ISyncAction) showPhysicalSlowActionClass.getConstructor(String.class)
                    .newInstance(executionContext.getSchemaName());
            } catch (Exception e) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());

            }
            List<List<Map<String, Object>>> results = SyncManagerHelper.sync(showPhysicalSlowAction,
                executionContext.getSchemaName());
            int size = 0;
            for (List<Map<String, Object>> rs : results) {
                if (rs == null) {
                    continue;
                }
                size += rs.size();
            }
            if (size == 0) {
                return result;
            }
            SQLRecorder recorder = new SQLRecorder(Math.min(size, 100));
            for (List<Map<String, Object>> rs : results) {
                if (rs == null) {
                    continue;
                }

                for (Map<String, Object> row : rs) {
                    SQLRecord record = new SQLRecord();
                    record.dataNode = DataTypes.StringType.convertFrom(row.get("GROUP_NAME"));
                    record.dbKey = DataTypes.StringType.convertFrom(row.get("DBKEY_NAME"));
                    record.port = DataTypes.StringType.convertFrom(row.get("PORT"));
                    record.schema = DataTypes.StringType.convertFrom(row.get("SCHEMA"));
                    record.startTime = DataTypes.LongType.convertFrom(row.get("START_TIME"));
                    record.executeTime = DataTypes.LongType.convertFrom(row.get("EXECUTE_TIME"));
                    record.affectRow = DataTypes.LongType.convertFrom(row.get("AFFECT_ROW"));
                    record.statement = DataTypes.StringType.convertFrom(row.get("SQL"));
                    record.sqlTime = DataTypes.LongType.convertFrom(row.get("SQL_EXECUTE_TIME"));
                    record.getLockConnectionTime = DataTypes.LongType.convertFrom(row.get("GETLOCK_CONNECTION_TIME"));
                    record.createConnectionTime = DataTypes.LongType.convertFrom(row.get("CREATE_CONNECTION_TIME"));
                    record.traceId = DataTypes.StringType.convertFrom(row.get("TRACE_ID"));
                    recorder.add(record);
                }
            }

            SQLRecord[] records = recorder.getSortedRecords();
            for (int i = records.length - 1; i >= 0; i--) {
                if (records[i] != null) {
                    result.addRow(new Object[] {
                        records[i].traceId, records[i].dataNode, records[i].dbKey,
                        new Date(records[i].startTime), records[i].executeTime, records[i].sqlTime,
                        records[i].getLockConnectionTime, records[i].createConnectionTime,
                        records[i].affectRow, records[i].statement});
                }
            }
        }

        return result;
    }
}
