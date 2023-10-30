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

package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.optimizer.planmanager.Statement;
import com.alibaba.polardbx.optimizer.planmanager.StatementMap;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExecuteStatement;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import org.apache.commons.collections.CollectionUtils;
import com.alibaba.polardbx.server.QueryResultHandler;

import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Created by simiao.zw on 2014/7/29.
 */
public final class ExecuteHandler {

    private static final Logger logger = LoggerFactory.getLogger(ExecuteHandler.class);

    /**
     * execute stmt1 using @a,@b;
     */
    public static boolean handle(ByteString stmt, ServerConnection c, int offset, boolean hasMore,
                                 QueryResultHandler handler) {
        try {
            List<Pair<Integer, ParameterContext>> params = new ArrayList<>();
            ByteString fullSql = parse(stmt, c.getSmForQuery(), c.getUserDefVariables(), params);
            /*
             * Here should let execute impersonate the SQL rather than EXECUTE,
             * since here should be a pure select like SQL, otherwise it will
             * run into binary result logic which only for COM_STMT_EXECUTE.
             */
            return c.execute(fullSql, hasMore, false, params, null, handler);
        } catch (SQLException e) {
            logger.error(e);
            c.writeErrMessage(e.getErrorCode(), null, e.getMessage());
            return false;
        }
    }

    public static ByteString parse(ByteString sql, StatementMap map, Map<String, Object> valmap,
                                   List<Pair<Integer, ParameterContext>> params) throws SQLException {
        try {
            MySqlExecuteStatement mySqlExecuteStatement = (MySqlExecuteStatement) FastsqlUtils.parseSql(sql).get(0);
            String name = mySqlExecuteStatement.getStatementName().getSimpleName();

            Statement stmt_define = map.find(name).getStmt();
            if (stmt_define == null) {
                throw new SQLSyntaxErrorException(
                    "Unknown prepared statement handler (" + name + ") given to EXECUTE",
                    "",
                    ErrorCode.ER_UNKNOWN_STMT_HANDLER.getCode());
            }

            ByteString fullSql = stmt_define.getRawSql();

            /*
             * fill parameters determine if parameters real value is
             * complex type, if so then directly merge into a single sql
             * to execute(no use cache)
             */
            processSimpleParameters(stmt_define, valmap, mySqlExecuteStatement.getParameters(), params);

            if (logger.isInfoEnabled()) {
                StringBuilder sqlInfo = new StringBuilder();
                sqlInfo.append("[execute] ");
                sqlInfo.append("[stmt_id:").append(name).append("] ");
                sqlInfo.append(sql);
                sqlInfo.append(", [");

                boolean first = true;
                for (Pair param : params) {
                    if (first) {
                        first = false;
                    } else {
                        sqlInfo.append(",");
                    }

                    sqlInfo.append(param.getValue());
                }

                sqlInfo.append("]");
                logger.info(sqlInfo.toString());
            }
            return fullSql;
        } catch (Exception e) {
            throw new SQLSyntaxErrorException("execute phase error", e);
        }
    }

    /**
     * return true if it's simple type parameters, otherwise return false, func
     * will clear non-complete output parameter if complex
     */
    private static void processSimpleParameters(Statement stmt, Map<String, Object> valmap,
                                                List<SQLExpr> userParameters,
                                                List<Pair<Integer, ParameterContext>> params) throws SQLException {
        if (stmt.getPrepareParamCount() > userParameters.size()) {
            throw new SQLException("Parameter is not enough");
        }

        for (int i = 0; i < stmt.getPrepareParamCount(); i++) {
            int paramIndex = i + 1;

            String nameWithPrefix = userParameters.get(i).toString();
            final String name;
            if (nameWithPrefix.startsWith("@@")) {
                name = nameWithPrefix.substring(2);
            } else if (nameWithPrefix.startsWith("@")) {
                name = nameWithPrefix.substring(1);
            } else {
                name = nameWithPrefix;
            }

            Object obj = valmap.get(name);

            // here should always be String for COM_QUERY execute
            obj = Objects.toString(obj);

            /*
             * QUERY version's param by SET will be stored into valmap, and
             * stmt.getParam is construct by API 's execute command, for
             * compatible for both methods, we will check if the current param
             * is empty. for COM_STMT_PREPARE, should not use set variables.
             */
            if (CollectionUtils.isNotEmpty(stmt.getParams())) {
                obj = stmt.getParam(i);
            }

            /*
             * use exact execute first packet data type to extract incoming data
             * and always use setObject1 to set java object to param context,
             * then the optimizer has the capability to recognize it.
             */
            params.add(new Pair<>(paramIndex, new ParameterContext(ParameterMethod.setObject1,
                new Object[] {paramIndex, Statement.processStringValue(obj)})));
        }
    }
}
