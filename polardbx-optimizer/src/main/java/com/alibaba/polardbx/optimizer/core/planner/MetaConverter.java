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

package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.planmanager.PreparedStmtCache;
import com.alibaba.polardbx.optimizer.planmanager.Statement;
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.parse.bean.FieldMetaData;
import com.alibaba.polardbx.optimizer.parse.bean.PreStmtMetaData;
import com.alibaba.polardbx.optimizer.parse.bean.TableMetaData;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameterKey;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.TDDLSqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlShuttle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 从SqlNode中得到Meta信息
 *
 * @author hongxi.chx
 */
public class MetaConverter {

    protected final static Logger logger = LoggerFactory.getLogger(MetaConverter.class);

    public static PreStmtMetaData getMetaData(PreparedStmtCache preparedStmtCache, List<?> params,
                                              ExecutionContext executionContext) throws SQLException {
        ByteString sql = preparedStmtCache.getStmt().getRawSql();
        // 解析获得参数个数
        ContextParameters contextParameters = new ContextParameters(executionContext.isTestMode());
        contextParameters.setPrepareMode(true);
        FastsqlParser fastsqlParser = new FastsqlParser();
        SqlNodeList sqlNodeList = fastsqlParser.parse(sql, params, contextParameters, executionContext);
        Integer parameter = contextParameters.getParameter(ContextParameterKey.ORIGIN_PARAMETER_COUNT);
        if (parameter != null && parameter > Short.MAX_VALUE) {
            throw new SQLException("Prepare parameter count is exceeded and the maximum is " + Short.MAX_VALUE);
        }

        // 从计划获得字段元信息
        if (null == executionContext.getParams()) {
            executionContext.setParams(new Parameters());
        }
        ExecutionPlan plan = getPlanInPrepare(sql, sqlNodeList, preparedStmtCache, executionContext);
        if (plan == null) {
            throw new SQLException("Unable to prepare sql: " + sql);
        }

        List<FieldMetaData> selectItems = convertFromPlan(plan);
        return new PreStmtMetaData(parameter, selectItems);
    }

    /**
     * Plan generated for a new query in the prepare phase will not be cached.
     * However, we can get plan from PlanCache once it is executed.
     */
    private static ExecutionPlan getPlanInPrepare(ByteString sql,
                                                  SqlNodeList sqlNodeList,
                                                  PreparedStmtCache preparedStmtCache,
                                                  ExecutionContext executionContext) {
        final ParamManager pm = executionContext.getParamManager();
        final Boolean enablePostPlanner = pm.getBoolean(ConnectionParams.ENABLE_POST_PLANNER);

        pm.getProps().put(ConnectionProperties.PREPARE_OPTIMIZE, Boolean.TRUE.toString());
        pm.getProps().put(ConnectionProperties.ENABLE_POST_PLANNER, Boolean.FALSE.toString());

        try {
            return Planner.getInstance().planForPrepare(sql, sqlNodeList, preparedStmtCache, executionContext);
        } finally {
            pm.getProps().put(ConnectionProperties.PREPARE_OPTIMIZE, Boolean.FALSE.toString());
            pm.getProps().put(ConnectionProperties.ENABLE_POST_PLANNER, String.valueOf(enablePostPlanner));
        }
    }

    private static List<FieldMetaData> convertFromPlan(ExecutionPlan plan) {
        List<FieldMetaData> selectItems = new ArrayList<>();
        CursorMeta cursorMeta = plan.getCursorMeta();
        assert cursorMeta != null;
        final List<ColumnMeta> columns = cursorMeta.getColumns();
        for (ColumnMeta columnMeta : columns) {
            final FieldMetaData fieldMetaData = FieldMetaData.copyFrom(columnMeta);
            selectItems.add(fieldMetaData);
        }
        return selectItems;
    }
}
