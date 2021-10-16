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

package com.alibaba.polardbx.optimizer.parse;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.ParserException;
import com.alibaba.polardbx.optimizer.core.profiler.cpu.CpuStat;
import com.google.common.annotations.VisibleForTesting;
import com.alibaba.polardbx.common.constants.CpuStatAttribute.CpuStatAttr;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.MetricLevel;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.exception.SqlParserException;
import com.alibaba.polardbx.optimizer.parse.custruct.FastSqlConstructUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameterKey;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 提供Fast SQL Parser AST到Calcite AST的转换 Calcite Parser based on FastSql Parser
 *
 * @author hongxi.chx on 2017/11/21.
 * @since 5.0.0
 */
public class FastsqlParser {

    public SqlNodeList parse(String sql) throws SqlParserException {
        return parse(ByteString.from(sql));
    }

    public SqlNodeList parse(final ByteString sql) throws SqlParserException {
        return parseWithStat(sql, null, null, null);
    }

    public SqlNodeList parse(final ByteString sql, ExecutionContext executionContext) throws SqlParserException {
        boolean testMode = executionContext == null ? false : executionContext.isTestMode();
        return parse(sql, null, new ContextParameters(testMode), executionContext);
    }

    public SqlNodeList parse(final String sql, ExecutionContext executionContext)
        throws SqlParserException {
        return parse(ByteString.from(sql), executionContext);
    }

    @VisibleForTesting
    public SqlNodeList parse(final ByteString sql, List<?> params) throws SqlParserException {
        return parseWithStat(sql, params, null, null);
    }

    public SqlNodeList parse(final String sql, List<?> params) throws SqlParserException {
        return parse(ByteString.from(sql), params);
    }

    public SqlNodeList parse(final String sql, List<?> params, ExecutionContext executionContext)
        throws SqlParserException {
        return parse(ByteString.from(sql), params, executionContext);
    }

    public SqlNodeList parse(final ByteString sql, List<?> params, ExecutionContext executionContext)
        throws SqlParserException {
        boolean testMode = executionContext == null ? false : executionContext.isTestMode();
        return parse(sql, params, new ContextParameters(testMode), executionContext);
    }

    public SqlNodeList parse(final ByteString sql, List<?> params, ContextParameters contextParameters,
                             ExecutionContext executionContext)
        throws SqlParserException {
        if (executionContext.getPrivilegeContext() != null) {
            contextParameters.setPrivilegeContext(executionContext.getPrivilegeContext());
        }
        SqlNodeList sqlNodeList = parseWithStat(sql, params, contextParameters, executionContext);
        return sqlNodeList;
    }

    protected SqlNodeList parseWithStat(final ByteString sql, List<?> params, ContextParameters contextParameters,
                                        ExecutionContext executionContext)
        throws SqlParserException {

        ContextParameters parserParameters = contextParameters;
        if (parserParameters == null) {
            boolean testMode = executionContext == null ? false : executionContext.isTestMode();
            parserParameters = new ContextParameters(testMode);
        }
        boolean enableTaskCpu = false;
        if (executionContext != null) {
            enableTaskCpu =
                MetricLevel.isSQLMetricEnabled(executionContext.getParamManager().getInt(
                    ConnectionParams.MPP_METRIC_LEVEL)) && executionContext.getRuntimeStatistics() != null;
            parserParameters.putParameter(ContextParameterKey.SCHEMA, executionContext.getSchemaName());
            parserParameters.setParameterNlsStrings(executionContext.getParameterNlsStrings());
        }

        SqlNodeList sqlNodeList = null;

        if (!enableTaskCpu) {
            sqlNodeList = doParse(sql, params, parserParameters, executionContext);
            if (parserParameters.isUseHint() && executionContext != null) {
                executionContext.setUseHint(true);
            }
            return sqlNodeList;
        }

        long startParseSqlNano = 0L;
        if (enableTaskCpu) {
            startParseSqlNano = ThreadCpuStatUtil.getThreadCpuTimeNano();
        }

        // parse sql
        sqlNodeList = doParse(sql, params, parserParameters, executionContext);

        if (parserParameters.isUseHint() && executionContext != null) {
            executionContext.setUseHint(true);
        }
        if (enableTaskCpu) {
            CpuStat cpuStat = executionContext.getRuntimeStatistics().getCpuStat();
            cpuStat.addCpuStatItem(CpuStatAttr.PARSE_SQL, ThreadCpuStatUtil.getThreadCpuTimeNano() - startParseSqlNano);
        }
        return sqlNodeList;
    }

    protected SqlNodeList doParse(final ByteString sql, List<?> params, ContextParameters contextParameters,
                                  ExecutionContext ec)
        throws SqlParserException {
        try {
            return realParse(sql, params, contextParameters, ec);
        } catch (Exception e) {
            throw new SqlParserException(e, e.getMessage());
        }
    }

    protected SqlNodeList realParse(ByteString sql, List<?> params, ContextParameters contextParameters,
                                    ExecutionContext ec) {
        try {
            List<SQLStatement> stmtList = FastsqlUtils.parseSql(sql);
            List<SqlNode> sqlNodes = new ArrayList<>();
            for (SQLStatement statement : stmtList) {
                final SqlNode converted;
                if (contextParameters == null) {
                    contextParameters = new ContextParameters();
                }
                converted = convertStatementToSqlNode(statement, params, contextParameters, ec);
                if (statement instanceof MySqlHintStatement && converted instanceof SqlNodeList) {
                    sqlNodes.addAll(((SqlNodeList) converted).getList());
                } else {
                    sqlNodes.add(converted);
                }
            }
            SqlNodeList sqlNodeList = new SqlNodeList(sqlNodes, SqlParserPos.ZERO);
            return sqlNodeList;
        } catch (ParserException e) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NOT_SUPPORT,
                e,
                "Not support and check the manual that corresponds to your CoronaDB server version");
        }
    }

    public static SqlNode convertStatementToSqlNode(SQLStatement statement, List<?> params, ExecutionContext ec) {
        return convertStatementToSqlNode(statement, params, new ContextParameters(false), ec);
    }

    public static SqlNode convertStatementToSqlNode(SQLStatement statement, List<?> params, ContextParameters context,
                                                    ExecutionContext ec) {
        context.setHeadHints(statement.getHeadHintsDirect());
        if (CollectionUtils.isNotEmpty(params)) {
            context.putParameter(ContextParameterKey.PARAMS, params);
        }

        FastSqlToCalciteNodeVisitor visitor = new FastSqlToCalciteNodeVisitor(context, ec);
        statement.accept(visitor);

        SqlNode result = visitor.getSqlNode();
//        if (context.isTestMode()) {
//            result = result.accept(new ReplaceTableNameWithTestTableVisitor(context.isTestMode()));
//        }

        if (null == result) {
            throw new FastSqlParserException(FastSqlParserException.ExceptionType.NEED_IMPLEMENT,
                "statement " + (statement != null ? statement.getClass().getName() : "null statement")
                    + " not supported");
        } else {
            if (result instanceof SqlHint && null != context.getHeadHints()) {
                final SqlHint sqlSupportHint = (SqlHint) result;
                if (null == sqlSupportHint.getHints() || sqlSupportHint.getHints().size() <= 0) {
                    final SqlNodeList hints = FastSqlConstructUtils.convertHints(context.getHeadHints(), context, ec);
                    sqlSupportHint.setHints(hints);
                }
            }
            return result;
        }
    }
}
