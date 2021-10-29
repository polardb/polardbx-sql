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
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.druid.sql.parser.SQLParserUtils;
import com.alibaba.polardbx.druid.sql.parser.SQLStatementParser;
import com.alibaba.polardbx.druid.sql.visitor.ParameterizedVisitor;
import com.alibaba.polardbx.druid.sql.visitor.VisitorFeature;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.parse.visitor.DrdsParameterizeSqlVisitor;
import com.alibaba.polardbx.optimizer.parse.visitor.ParamCountVisitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by hongxi.chx on 2017/12/1.
 */
public class SqlParameterizeUtils {

    private final static SQLParserFeature[] parserFeatures = {
        SQLParserFeature.EnableSQLBinaryOpExprGroup, SQLParserFeature.OptimizedForParameterized,
        SQLParserFeature.TDDLHint, SQLParserFeature.EnableCurrentUserExpr, SQLParserFeature.DRDSAsyncDDL,
        SQLParserFeature.DRDSBaseline, SQLParserFeature.DrdsMisc, SQLParserFeature.DrdsGSI, SQLParserFeature.DrdsCCL
    };

    private final static VisitorFeature[] parameterizeFeatures = {
        VisitorFeature.OutputParameterizedQuesUnMergeInList,
        VisitorFeature.OutputParameterizedUnMergeShardingTable,
        VisitorFeature.OutputParameterizedQuesUnMergeValuesList,
        VisitorFeature.OutputParameterizedQuesUnMergeOr,
        VisitorFeature.OutputParameterizedQuesUnMergeAnd
    };

    public static SqlParameterized parameterize(String sql) {
        return parameterize(sql, null, false);
    }

    public static SqlParameterized parameterize(String sql, boolean forPrepare) {
        return parameterize(sql, null, forPrepare);
    }

    public static SqlParameterized parameterize(String sql, Map<Integer, ParameterContext> params) {
        return parameterize(ByteString.from(sql), params, new ExecutionContext(), false);
    }

    public static SqlParameterized parameterize(String sql, Map<Integer, ParameterContext> params, boolean forPrepare) {
        return parameterize(ByteString.from(sql), params, new ExecutionContext(), forPrepare);
    }

    public static SqlParameterized parameterize(ByteString sql,
                                                Map<Integer, ParameterContext> parameters,
                                                ExecutionContext executionContext,
                                                boolean forPrepare) {
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, JdbcConstants.MYSQL,
            SqlParameterizeUtils.parserFeatures);

        List<SQLStatement> statements = parser.parseStatementList();
        if (statements.size() == 0) {
            return null;
        }
        final SQLStatement statement = statements.get(0);
        return parameterize(sql, statement, parameters, executionContext, forPrepare);
    }

    public static SqlParameterized parameterize(ByteString sql, SQLStatement statement,
                                                Map<Integer, ParameterContext> parameters,
                                                ExecutionContext executionContext,
                                                boolean forPrepare) {
        // prepare mode
        if (parameters != null && parameters.size() > 0) {
            List<Object> tmpParameters = new ArrayList<>();
            ParamCountVisitor paramCountVisitor = new ParamCountVisitor();
            statement.accept(paramCountVisitor);
            int parameterCount = paramCountVisitor.getParameterCount();
            for (int i = 0; i < parameterCount; i++) {
                tmpParameters.add(parameters.get(i + 1).getValue());
            }
            // by hongxi.chx
            // Use the original sql instead of parameterized sql. Because
            // the order of parameters corresponds to the original sql. For
            // example, originalSql = 'select * from tb limit ? offset ?',
            // params = '10, 20', parameterizedSql = 'select * from tb limit ?, ?'.
            return SqlParameterizeUtils.parameterizeStmt(statement, sql, executionContext, tmpParameters);
        } else {
            // Executing prepare statement should use the same parameterization as common queries
            // Use the original sql in prepare phase
            // so that it can hit cache with the same cache key in execute phase
            return SqlParameterizeUtils.parameterizeStmt(statement, sql, executionContext);
        }
    }

    public static boolean needCache(SQLStatement stmt) {
        return stmt instanceof SQLSelectStatement
            || stmt instanceof SQLInsertStatement
            || stmt instanceof SQLReplaceStatement
            || stmt instanceof SQLUpdateStatement
            || stmt instanceof SQLDeleteStatement;
    }

    private static SqlParameterized parameterizeStmt(SQLStatement stmt, ByteString sql,
                                                     ExecutionContext executionContext) {
        return parameterizeStmt(stmt, sql, executionContext, null);
    }

    private static SqlParameterized parameterizeStmt(SQLStatement stmt, ByteString sql,
                                                     ExecutionContext executionContext, List<Object> tmpParameters) {

        if (!needCache(stmt)) {
            // use sql instead of stmt.toString(), because fastsql special sql toString is buggy.
            return new SqlParameterized(sql, sql.toString(), new ArrayList<>(), stmt);
        }

        if (stmt.hasBeforeComment()) {
            stmt.getBeforeCommentsDirect().clear();
        }
        List<Object> outParameters = new ArrayList<>();

        StringBuilder out = new StringBuilder();
        DrdsParameterizeSqlVisitor visitor = new DrdsParameterizeSqlVisitor(out, true, executionContext);
        visitor.setTmpParameters(tmpParameters);
        visitor.setOutputParameters(outParameters);
        configVisitorFeatures(visitor, parameterizeFeatures);

        stmt.accept(visitor);
        String s = out.toString();
        return new SqlParameterized(sql, s, Lists.newArrayList(outParameters), stmt);
    }

    private static void configVisitorFeatures(ParameterizedVisitor visitor, VisitorFeature... features) {
        if (features != null) {
            for (VisitorFeature feature : features) {
                visitor.config(feature, true);
            }
        }
    }
}
