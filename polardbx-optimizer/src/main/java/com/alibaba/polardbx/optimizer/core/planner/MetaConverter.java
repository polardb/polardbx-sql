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

import com.alibaba.polardbx.ErrorCode;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.planmanager.PreparedStmtCache;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.parse.bean.FieldMetaData;
import com.alibaba.polardbx.optimizer.parse.bean.PreStmtMetaData;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameterKey;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 从SqlNode中得到Meta信息
 *
 * @author hongxi.chx
 */
public class MetaConverter {

    protected final static Logger logger = LoggerFactory.getLogger(MetaConverter.class);
    /**
     * prepare参数个数用2字节表示
     */
    private static final int MAX_PREPARED_PARAM_COUNT = 0xFFFF;

    public static PreStmtMetaData getMetaData(PreparedStmtCache preparedStmtCache, List<?> params,
                                              ExecutionContext executionContext) throws SQLException {
        ByteString sql = preparedStmtCache.getStmt().getRawSql();
        // 解析获得参数个数
        ContextParameters contextParameters = new ContextParameters(executionContext.isTestMode());
        contextParameters.setPrepareMode(true);
        FastsqlParser fastsqlParser = new FastsqlParser();
        SqlNodeList sqlNodeList = fastsqlParser.parse(sql, params, contextParameters, executionContext);
        Integer parameterCnt = contextParameters.getParameter(ContextParameterKey.ORIGIN_PARAMETER_COUNT);
        if (parameterCnt != null && parameterCnt > MAX_PREPARED_PARAM_COUNT) {
            throw new SQLException(
                "Prepared statement contains too many placeholders. The maximum is " + MAX_PREPARED_PARAM_COUNT);
        }
        if (sqlNodeList.size() > 1) {
            // should not be here, since it's handled in earlier steps
            throw new SQLException("You have an error in your SQL syntax", "42000", ErrorCode.ER_PARSE_ERROR);
        }

        // 从计划获得字段元信息
        // 并保存参数化sql到prepare缓存中
        executionContext.setParams(new Parameters());
        ExecutionPlan plan = getPlanInPrepare(sql, sqlNodeList, preparedStmtCache, executionContext);
        if (plan == null) {
            throw new SQLException("Unable to prepare sql: " + sql);
        }

        SqlNode ast = sqlNodeList.get(0);
        if (ast.getKind() != SqlKind.UNION && ast.getKind() != SqlKind.SELECT) {
            // 只有DQL才返回字段元信息
            return new PreStmtMetaData(parameterCnt, new ArrayList<>());
        }

        List<FieldMetaData> selectItems = convertFromPlan(plan);
        return new PreStmtMetaData(parameterCnt, selectItems);
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
            return Planner.getInstance().planForPrepare(sql, preparedStmtCache, executionContext);
        } finally {
            pm.getProps().put(ConnectionProperties.PREPARE_OPTIMIZE, Boolean.FALSE.toString());
            pm.getProps().put(ConnectionProperties.ENABLE_POST_PLANNER, String.valueOf(enablePostPlanner));
        }
    }

    private static List<FieldMetaData> convertFromPlan(ExecutionPlan plan) {
        List<FieldMetaData> selectItems = new ArrayList<>();
        CursorMeta cursorMeta = plan.getCursorMeta();
        if (cursorMeta == null) {
            return selectItems;
        }
        final List<ColumnMeta> columns = cursorMeta.getColumns();
        for (ColumnMeta columnMeta : columns) {
            final FieldMetaData fieldMetaData = FieldMetaData.copyFrom(columnMeta);
            selectItems.add(fieldMetaData);
        }
        return selectItems;
    }
}
