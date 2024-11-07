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

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
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
import org.apache.calcite.sql.SqlBaseline;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by hongxi.chx on 2017/12/1.
 */
public class SqlParameterizeUtils {
    public final static VisitorFeature[] parameterizeFeatures = {
//        VisitorFeature.OutputParameterizedQuesUnMergeInList,
        VisitorFeature.OutputParameterizedSpecialNameWithBackTick,
        VisitorFeature.OutputParameterizedUnMergeShardingTable,
        VisitorFeature.OutputParameterizedQuesUnMergeValuesList,
        VisitorFeature.OutputParameterizedQuesUnMergeOr,
        VisitorFeature.OutputParameterizedQuesUnMergeAnd
    };

    private final static VisitorFeature[] parameterizeFeaturesForPrepare = {
        VisitorFeature.OutputParameterizedSpecialNameWithBackTick,
        VisitorFeature.OutputParameterizedQuesUnMergeInList,
        VisitorFeature.OutputParameterizedUnMergeShardingTable,
        VisitorFeature.OutputParameterizedQuesUnMergeValuesList,
        VisitorFeature.OutputParameterizedQuesUnMergeOr,
        VisitorFeature.OutputParameterizedQuesUnMergeAnd
    };

    public static SqlParameterized parameterize(String sql, boolean isPrepare) {
        return parameterize(sql, null, isPrepare);
    }


    private final static VisitorFeature[] parameterizeFeaturesForMergeIn = {
        VisitorFeature.OutputParameterizedUnMergeShardingTable,
        VisitorFeature.OutputParameterizedQuesUnMergeValuesList,
        VisitorFeature.OutputParameterizedQuesUnMergeOr,
        VisitorFeature.OutputParameterizedQuesUnMergeAnd
    };

    public static String parameterizeStmtMergeIn(SQLStatement stmt, ExecutionContext executionContext) {
        if (stmt.hasBeforeComment()) {
            stmt.getBeforeCommentsDirect().clear();
        }
        List<Object> outParameters = new ArrayList<>();

        StringBuilder out = new StringBuilder();
        ParameterizedVisitor visitor = new DrdsParameterizeSqlVisitor(out, true, executionContext);
        visitor.setOutputParameters(outParameters);
        configVisitorFeatures(visitor, parameterizeFeaturesForMergeIn);

        stmt.accept(visitor);
        return out.toString();
    }

    public static String parameterizeSqlMergeIn(String sql, ExecutionContext executionContext) {
        SqlParameterized sqlParameterized = parameterize(sql);
        return parameterizeStmtMergeIn(sqlParameterized.getStmt(), executionContext);
    }

    public static SqlParameterized parameterize(String sql) {
        return parameterize(sql, null, false);
    }

    public static SqlParameterized parameterize(String sql, Map<Integer, ParameterContext> params, boolean isPrepare) {
        return parameterize(ByteString.from(sql), params, new ExecutionContext(), isPrepare);
    }

    public static SqlParameterized parameterize(ByteString sql,
                                                Map<Integer, ParameterContext> parameters,
                                                ExecutionContext executionContext, boolean isPrepare) {
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(sql, JdbcConstants.MYSQL,
            SQLUtils.parserFeatures);

        try {
            List<SQLStatement> statements = parser.parseStatementList();
            if (statements.size() == 0) {
                return null;
            }
            int lineNum = parser.getLexer().getLine();
            sql.setMultiLine(lineNum >= 1);
            final SQLStatement statement = statements.get(0);
            return parameterize(sql, statement, parameters, executionContext, isPrepare);
        } catch (Throwable t) {
            if (ErrorCode.match(t.getMessage())) {
                throw t;
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_PARSER, t, t.getMessage());
            }
        }
    }

    public static SqlParameterized parameterize(ByteString sql, SQLStatement statement,
                                                Map<Integer, ParameterContext> parameters,
                                                ExecutionContext executionContext, boolean isPrepare) {
        // prepare mode
        if (parameters != null && parameters.size() > 0) {
            /*
                inner prepare mode
                系统内部的prepare语句直接将参数保存到
                executionContext.params中
             */
            List<Object> tmpParameters = new ArrayList<>();
            ParamCountVisitor paramCountVisitor = new ParamCountVisitor();
            statement.accept(paramCountVisitor);
            int parameterCount = paramCountVisitor.getParameterCount();
            for (int i = 0; i < parameterCount; i++) {
                tmpParameters.add(parameters.get(i + 1).getValue());
            }
            return new SqlParameterized(sql, sql.toString(), tmpParameters, statement, true);
        } else {
            return SqlParameterizeUtils.parameterizeStmt(statement, sql, executionContext, isPrepare);
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
                                                     ExecutionContext executionContext, boolean isPrepare) {
        if (!needCache(stmt)) {
            // use sql instead of stmt.toString(), because fastsql special sql toString is buggy.
            return new SqlParameterized(sql, sql.toString(), new ArrayList<>(), stmt, true);
        }

        if (stmt.hasBeforeComment()) {
            stmt.getBeforeCommentsDirect().clear();
        }
        List<Object> outParameters = new ArrayList<>();

        StringBuilder out = new StringBuilder();
        DrdsParameterizeSqlVisitor visitor = new DrdsParameterizeSqlVisitor(out, true, executionContext);
        visitor.setOutputParameters(outParameters);
        // for parameterize in expr
        if (isPrepare) {
            configVisitorFeatures(visitor, parameterizeFeaturesForPrepare);
        } else {
            configVisitorFeatures(visitor, parameterizeFeatures);
            visitor.setParameterizedMergeInList(true);
        }

        stmt.accept(visitor);
        String s = out.toString();
        return new SqlParameterized(sql, s, Lists.newArrayList(outParameters), stmt, false);
    }

    private static void configVisitorFeatures(ParameterizedVisitor visitor, VisitorFeature... features) {
        if (features != null) {
            for (VisitorFeature feature : features) {
                visitor.config(feature, true);
            }
        }
    }

}
