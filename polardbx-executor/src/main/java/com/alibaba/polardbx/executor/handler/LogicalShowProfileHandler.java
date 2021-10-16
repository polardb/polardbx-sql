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
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlShowProfile;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class LogicalShowProfileHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalShowProfileHandler.class);

    public static final String PROFILE_ATTR_NODE_HOST = "NODE_HOST";
    public static final String PROFILE_ATTR_CONN_ID = "CONN_ID";
    public static final String PROFILE_ATTR_TRACE_ID = "TRACE_ID";
    public static final String PROFILE_ATTR_DB = "DB";

    public static final String PROFILE_ATTR_TIME_COST = "TIME_COST(ns)";
    public static final String PROFILE_ATTR_TIME_COST_PCT = "PCT";

    public static final String PROFILE_ATTR_LOG_TIME_COST = "LOG_TC(ns)";
    public static final String PROFILE_ATTR_PHY_SQL_TIME_COST = "PHY_SQL_TC(ns)";
    public static final String PROFILE_ATTR_PHY_RS_TIME_COST = "PHY_RS_TC(ns)";

    public static final String PROFILE_ATTR_SQL = "SQL";
    public static final String PROFILE_ATTR_STAGE = "STAGE";
    public static final String PROFILE_ATTR_ROW_COUNT = "ROW_COUNT";

    public static final String PROFILE_ATTR_MEMORY_POOL_NAME = "POOL_NAME";
    public static final String PROFILE_ATTR_MEMORY_USED = "USED(byte)";
    public static final String PROFILE_ATTR_MEMORY_PEAK = "USED_PEAK(byte)";
    public static final String PROFILE_ATTR_MEMORY_LIMIT = "LIMIT(byte)";
    public static final String PROFILE_ATTR_MEMORY_PCT = "PCT";

    public static final String PROFILE_TOTAL = "Total";
    public static final String PROFILE_INDENTS_UNIT = "  ";
    public static final String PROFILE_TABLE_NAME = "SQL_PROFILE";
    public static final String PROFILE_VALUE_COMPUTING = "computing";
    public static final String PROFILE_NO_VALUE = "--";
    public static final String PROFILE_ZEOR_VALUE = "0";
    public static final String PROFILE_ZEOR_PCT_VALUE = "0.0000%";
    public static final String PROFILE_CACHED_POSTFIX = "(Cached)";
    public static final String PROFILE_PLAN_STATE_RUNNING = ", Executing";
    public static final String PROFILE_PLAN_STATE_COMPLETED = "";
    public static final String PROFILE_PLAN_PARALLELISM = "(Parallelism:%s)";
    public static final String PROFILE_TRACE_ID = "(TraceId:%s%s)";

    static Class showProfileSyncActionClass;

    static {

        // 只有server支持，这里是暂时改法，后续要将这段逻辑解耦
        try {
            showProfileSyncActionClass = Class.forName("com.alibaba.polardbx.server.response.ShowProfileSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public LogicalShowProfileHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {

        if (showProfileSyncActionClass == null) {
            throw new NotSupportException();
        }

        SqlShowProfile sqlShowProfile = (SqlShowProfile) ((LogicalShow) logicalPlan).getNativeSqlNode();
        SqlNode forQuery = sqlShowProfile.getForQuery();
        String forQueryId = null;
        if (forQuery instanceof SqlNumericLiteral) {
            forQueryId = String.valueOf(((SqlNumericLiteral) forQuery).getValue());
            long forQueryIdInt = Long.valueOf(forQueryId);
            if (forQueryIdInt == 0) {
                long connId = (Long) executionContext.getConnId();
                forQueryId = String.valueOf(connId);
            }

        }
        List<String> typeList = sqlShowProfile.getTypes();

        String type = typeList.get(0);
        if (type.equalsIgnoreCase(SqlShowProfile.MEMORY_TYPE)) {
            return doShowMemoryProfile(sqlShowProfile, forQueryId, typeList, executionContext);
        } else {
            return doShowCpuProfile(sqlShowProfile, forQueryId, typeList, executionContext);
        }
    }

    protected Cursor doShowCpuProfile(SqlNode sqlShowProfile, String forQueryId, List<String> typeList,
                                      ExecutionContext executionContext) {

        String forQuery = forQueryId;
        ArrayResultCursor result = null;
        if (forQuery == null) {
            result = new ArrayResultCursor(PROFILE_TABLE_NAME);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_CONN_ID, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_TRACE_ID, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_DB, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_LOG_TIME_COST, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_PHY_SQL_TIME_COST, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_PHY_RS_TIME_COST, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_SQL, DataTypes.StringType);
            result.initMeta();

            List<List<Map<String, Object>>> syncResults = new ArrayList<>();
            try {
                syncResults = doShowProfileSyncAction(executionContext.getSchemaName(), forQueryId, typeList);
            } catch (Throwable ex) {
                logger.warn(ex);
            }
            for (List<Map<String, Object>> nodeRows : syncResults) {
                if (nodeRows == null) {
                    continue;
                }
                for (Map<String, Object> row : nodeRows) {
                    result.addRow(new Object[] {
                        DataTypes.ULongType.convertJavaFrom(row.get(LogicalShowProfileHandler.PROFILE_ATTR_CONN_ID)),
                        DataTypes.StringType.convertFrom(row.get(LogicalShowProfileHandler.PROFILE_ATTR_TRACE_ID)),
                        DataTypes.StringType.convertFrom(row.get(LogicalShowProfileHandler.PROFILE_ATTR_DB)),
                        DataTypes.StringType.convertFrom(row.get(LogicalShowProfileHandler.PROFILE_ATTR_LOG_TIME_COST)),
                        DataTypes.StringType.convertFrom(
                            row.get(LogicalShowProfileHandler.PROFILE_ATTR_PHY_SQL_TIME_COST)),
                        DataTypes.StringType.convertFrom(
                            row.get(LogicalShowProfileHandler.PROFILE_ATTR_PHY_RS_TIME_COST)),
                        DataTypes.StringType.convertFrom(row.get(LogicalShowProfileHandler.PROFILE_ATTR_SQL))});
                }
            }
        } else {

            result = new ArrayResultCursor(PROFILE_TABLE_NAME);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_STAGE, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_TIME_COST, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_TIME_COST_PCT, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_ROW_COUNT, DataTypes.StringType);
            result.initMeta();

            List<List<Map<String, Object>>> syncResults = new ArrayList<>();
            try {
                syncResults = doShowProfileSyncAction(executionContext.getSchemaName(), forQueryId, typeList);
            } catch (Throwable ex) {
                logger.warn(ex);
            }
            for (List<Map<String, Object>> nodeRows : syncResults) {
                if (nodeRows == null) {
                    continue;
                }
                for (Map<String, Object> row : nodeRows) {
                    result.addRow(new Object[] {
                        DataTypes.StringType.convertFrom(row.get(LogicalShowProfileHandler.PROFILE_ATTR_STAGE)),
                        DataTypes.StringType.convertFrom(row.get(LogicalShowProfileHandler.PROFILE_ATTR_TIME_COST)),
                        DataTypes.StringType.convertFrom(row.get(LogicalShowProfileHandler.PROFILE_ATTR_TIME_COST_PCT)),
                        DataTypes.StringType.convertFrom(row.get(LogicalShowProfileHandler.PROFILE_ATTR_ROW_COUNT)),});
                }
            }
        }

        return result;
    }

    protected Cursor doShowMemoryProfile(SqlNode sqlShowProfile, String forQueryId, List<String> typeList,
                                         ExecutionContext executionContext) {

        String forQuery = forQueryId;
        ArrayResultCursor result = null;
        if (forQuery == null) {
            result = new ArrayResultCursor(PROFILE_TABLE_NAME);

            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_MEMORY_POOL_NAME, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_CONN_ID, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_NODE_HOST, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_MEMORY_USED, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_MEMORY_PEAK, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_MEMORY_LIMIT, DataTypes.StringType);

            List<List<Map<String, Object>>> syncResults = doShowProfileSyncAction(executionContext.getSchemaName(),
                forQueryId, typeList);
            for (List<Map<String, Object>> nodeRows : syncResults) {
                if (nodeRows == null) {
                    continue;
                }
                for (Map<String, Object> row : nodeRows) {
                    result.addRow(new Object[] {
                        DataTypes.StringType.convertFrom(
                            row.get(LogicalShowProfileHandler.PROFILE_ATTR_MEMORY_POOL_NAME)),
                        DataTypes.StringType.convertFrom(row.get(LogicalShowProfileHandler.PROFILE_ATTR_CONN_ID)),
                        DataTypes.StringType.convertFrom(row.get(LogicalShowProfileHandler.PROFILE_ATTR_NODE_HOST)),
                        DataTypes.StringType.convertFrom(row.get(LogicalShowProfileHandler.PROFILE_ATTR_MEMORY_USED)),
                        DataTypes.StringType.convertFrom(row.get(LogicalShowProfileHandler.PROFILE_ATTR_MEMORY_PEAK)),
                        DataTypes.StringType.convertFrom(
                            row.get(LogicalShowProfileHandler.PROFILE_ATTR_MEMORY_LIMIT))});
                }
            }
        } else {

            result = new ArrayResultCursor(PROFILE_TABLE_NAME);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_STAGE, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_MEMORY_USED, DataTypes.StringType);
            result.addColumn(LogicalShowProfileHandler.PROFILE_ATTR_MEMORY_PEAK, DataTypes.StringType);

            List<List<Map<String, Object>>> syncResults = doShowProfileSyncAction(executionContext.getSchemaName(),
                forQueryId, typeList);
            for (List<Map<String, Object>> nodeRows : syncResults) {
                if (nodeRows == null) {
                    continue;
                }
                for (Map<String, Object> row : nodeRows) {
                    result.addRow(new Object[] {
                        DataTypes.StringType.convertFrom(row.get(LogicalShowProfileHandler.PROFILE_ATTR_STAGE)),
                        DataTypes.StringType.convertFrom(row.get(LogicalShowProfileHandler.PROFILE_ATTR_MEMORY_USED)),
                        DataTypes.StringType.convertFrom(
                            row.get(LogicalShowProfileHandler.PROFILE_ATTR_MEMORY_PEAK)),});
                }
            }
        }

        return result;
    }

    protected List<List<Map<String, Object>>> doShowProfileSyncAction(String schemaName, String forQueryId,
                                                                      List<String> typeList) {
        ISyncAction showProfileSyncAction;
        try {
            showProfileSyncAction = (ISyncAction) showProfileSyncActionClass.getConstructor(List.class, String.class)
                .newInstance(typeList, forQueryId);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
        List<List<Map<String, Object>>> results = SyncManagerHelper.sync(showProfileSyncAction, schemaName);
        return results;
    }
}
