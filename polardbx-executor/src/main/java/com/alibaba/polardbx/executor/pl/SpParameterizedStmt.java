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

package com.alibaba.polardbx.executor.pl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLValuesQuery;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLValuesTableSource;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.util.SpParameter;
import com.alibaba.polardbx.optimizer.planmanager.Statement;
import com.alibaba.polardbx.rpc.compatible.XPreparedStatement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SpParameterizedStmt {
    String parameterizedStmt;
    List<org.apache.calcite.util.Pair<String, SpParameter>> spParameters;

    public SpParameterizedStmt(String parameterizedStmt,
                               List<org.apache.calcite.util.Pair<String, SpParameter>> spParameters) {
        this.parameterizedStmt = parameterizedStmt;
        this.spParameters = spParameters;
    }

    public SpParameterizedStmt(String parameterizedStmt) {
        this.parameterizedStmt = parameterizedStmt;
        this.spParameters = new ArrayList<>();
    }

    public List<Pair<Integer, ParameterContext>> getParams() {
        int paramCount = spParameters.size();
        List<Pair<Integer, ParameterContext>> params = new ArrayList<>(paramCount);
        for (int paramIndex = 0; paramIndex < paramCount; ++paramIndex) {
            int index = paramIndex + 1;
            params.add(new Pair<>(index, new ParameterContext(ParameterMethod.setObject1,
                new Object[] {
                    index,
                    spParameters.get(paramIndex).getValue().getCurrentValue()})));
        }
        return params;
    }

    public Map<Integer, ParameterContext> getParamsForPlan() {
        int paramCount = spParameters.size();
        Map<Integer, ParameterContext> params = new HashMap<>(paramCount);
        for (int paramIndex = 0; paramIndex < paramCount; ++paramIndex) {
            int index = paramIndex + 1;
            params.put(index, new ParameterContext(ParameterMethod.setObject1,
                new Object[] {
                    index, spParameters.get(paramIndex).getValue().getCurrentValue()}));
        }
        return params;
    }

    public String getSql() {
        StringBuilder sb = new StringBuilder();
        List<Integer> placeHolderPos = getPlaceHolder(parameterizedStmt);
        int begin = 0, end;
        for (int i = 0; i < placeHolderPos.size(); ++i) {
            end = placeHolderPos.get(i);
            sb.append(parameterizedStmt, begin, end);
            // replace placeholder with real value
            sb.append(PLUtils.getPrintString(spParameters.get(i).getValue().getCurrentValue()));
            // skip "?"
            begin = end + 1;
        }
        end = parameterizedStmt.length();
        sb.append(parameterizedStmt, begin, end);
        return sb.toString();
    }

    private static List<Integer> getPlaceHolder(String sql) {
        List<Integer> placeHolderPos = new ArrayList<>();
        // Dealing sql.
        final int startPos = XPreparedStatement.findStartOfStatement(sql);
        final int statementLength = sql.length();
        final char quotedIdentifierChar = '`';

        boolean inQuotes = false;
        char quoteChar = 0;
        boolean inQuotedId = false;

        for (int i = startPos; i < statementLength; ++i) {
            char c = sql.charAt(i);

            if (c == '\\' && i < (statementLength - 1)) {
                i++;
                continue; // next character is escaped
            }

            // are we in a quoted identifier? (only valid when the id is not inside a 'string')
            if (!inQuotes && (c == quotedIdentifierChar)) {
                inQuotedId = !inQuotedId;
            } else if (!inQuotedId) {
                //	only respect quotes when not in a quoted identifier

                if (inQuotes) {
                    if (((c == '\'') || (c == '"')) && c == quoteChar) {
                        if (i < (statementLength - 1) && sql.charAt(i + 1) == quoteChar) {
                            i++;
                            continue; // inline quote escape
                        }

                        inQuotes = false;
                        quoteChar = 0;
                    }
                } else {
                    if (c == '#' || (c == '-' && (i + 1) < statementLength && sql.charAt(i + 1) == '-')) {
                        // run out to end of statement, or newline, whichever comes first
                        int endOfStmt = statementLength - 1;

                        for (; i < endOfStmt; i++) {
                            c = sql.charAt(i);

                            if (c == '\r' || c == '\n') {
                                break;
                            }
                        }

                        continue;
                    } else if (c == '/' && (i + 1) < statementLength) {
                        // Comment?
                        char cNext = sql.charAt(i + 1);

                        if (cNext == '*') {
                            i += 2;

                            for (int j = i; j < statementLength; j++) {
                                i++;
                                cNext = sql.charAt(j);

                                if (cNext == '*' && (j + 1) < statementLength) {
                                    if (sql.charAt(j + 1) == '/') {
                                        i++;

                                        if (i < statementLength) {
                                            c = sql.charAt(i);
                                        }

                                        break; // comment done
                                    }
                                }
                            }
                        }
                    } else if ((c == '\'') || (c == '"')) {
                        inQuotes = true;
                        quoteChar = c;
                    }
                }
            }

            if ((c == '?') && !inQuotes && !inQuotedId) {
                // Placeholder.
                placeHolderPos.add(i);
            }
        }
        return placeHolderPos;
    }

    public String getSelectParameterizedSql() {
        SQLSelectStatement statement = (SQLSelectStatement) FastsqlUtils.parseSql(parameterizedStmt).get(0);
        int paramIndex = 0;
        List<SQLSelectItem> selectItems = getSelectItemList(statement);
        for (int i = 0; i < selectItems.size(); ++i) {
            // have alias, don't need to set alias
            if (selectItems.get(i).getAlias() != null) {
                continue;
            }
            String item = selectItems.get(i).getExpr().toString();
            List<Integer> placeHolderPos = getPlaceHolder(item);
            if (placeHolderPos.isEmpty()) {
                continue;
            }
            int begin = 0, end;
            StringBuilder sb = new StringBuilder();
            for (int index = 0; index < placeHolderPos.size(); ++index) {
                end = placeHolderPos.get(index);
                sb.append(item, begin, end);
                // replace placeholder with real name
                sb.append(spParameters.get(paramIndex++).getKey());
                // skip "?"
                begin = end + 1;
            }
            end = item.length();
            sb.append(item, begin, end);
            selectItems.get(i).setAlias(sb.toString());
            selectItems.get(i).setForceQuotaAlias(true);
        }
        return statement.toString();
    }

    public List<SQLSelectItem> getSelectItemList(SQLSelectStatement statement) {
        SQLSelectQuery query = statement.getSelect().getQuery();
        // TODO: Notice: SQLValuesQuery is added in mysql8.0, we not support right now!
        if (query instanceof SQLValuesQuery || query instanceof SQLValuesTableSource) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "SqlValuesQuery is not implemented!");
        }
        if (query instanceof SQLUnionQuery) {
            query = ((SQLUnionQuery) query).getFirstQueryBlock();
        }
        // double check
        if (!(query instanceof SQLSelectQueryBlock)) {
            throw new TddlRuntimeException(ErrorCode.ERR_PROCEDURE_EXECUTE,
                "select type not support : " + query.getClass().getSimpleName());
        }
        return ((SQLSelectQueryBlock) query).getSelectList();
    }

    public String getParameterString() {
        return parameterizedStmt;
    }
}
