/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.druid.sql;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.FastsqlException;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLLimit;
import com.alibaba.polardbx.druid.sql.ast.SQLOrderBy;
import com.alibaba.polardbx.druid.sql.ast.SQLSetQuantifier;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAggregateOption;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelect;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;

import java.util.List;

public class PagerUtils {

    public static String count(String sql, DbType dbType) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        if (stmtList.size() != 1) {
            throw new IllegalArgumentException("sql not support count : " + sql);
        }

        SQLStatement stmt = stmtList.get(0);

        if (!(stmt instanceof SQLSelectStatement)) {
            throw new IllegalArgumentException("sql not support count : " + sql);
        }

        SQLSelectStatement selectStmt = (SQLSelectStatement) stmt;
        return count(selectStmt.getSelect(), dbType);
    }

    public static String limit(String sql, DbType dbType, int offset, int count) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        if (stmtList.size() != 1) {
            throw new IllegalArgumentException("sql not support count : " + sql);
        }

        SQLStatement stmt = stmtList.get(0);

        if (!(stmt instanceof SQLSelectStatement)) {
            throw new IllegalArgumentException("sql not support count : " + sql);
        }

        SQLSelectStatement selectStmt = (SQLSelectStatement) stmt;

        return limit(selectStmt.getSelect(), dbType, offset, count);
    }

    public static String limit(String sql, DbType dbType, int offset, int count, boolean check) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        if (stmtList.size() != 1) {
            throw new IllegalArgumentException("sql not support count : " + sql);
        }

        SQLStatement stmt = stmtList.get(0);

        if (!(stmt instanceof SQLSelectStatement)) {
            throw new IllegalArgumentException("sql not support count : " + sql);
        }

        SQLSelectStatement selectStmt = (SQLSelectStatement) stmt;

        limit(selectStmt.getSelect(), dbType, offset, count, check);

        return selectStmt.toString();
    }

    public static String limit(SQLSelect select, DbType dbType, int offset, int count) {
        limit(select, dbType, offset, count, false);

        return SQLUtils.toSQLString(select, dbType);
    }

    public static boolean limit(SQLSelect select, DbType dbType, int offset, int count, boolean check) {
        SQLSelectQuery query = select.getQuery();

        switch (dbType) {
        default:
            if (query instanceof SQLSelectQueryBlock) {
                return limitQueryBlock(select, dbType, offset, count, check);
            } else if (query instanceof SQLUnionQuery) {
                return limitUnion((SQLUnionQuery) query, dbType, offset, count, check);
            }
            throw new UnsupportedOperationException();
        }
    }

    private static boolean limitUnion(SQLUnionQuery queryBlock, DbType dbType, int offset, int count, boolean check) {
        SQLLimit limit = queryBlock.getLimit();
        if (limit != null) {
            if (offset > 0) {
                limit.setOffset(new SQLIntegerExpr(offset));
            }

            if (check && limit.getRowCount() instanceof SQLNumericLiteralExpr) {
                int rowCount = ((SQLNumericLiteralExpr) limit.getRowCount()).getNumber().intValue();
                if (rowCount <= count && offset <= 0) {
                    return false;
                }
            } else if (check && limit.getRowCount() instanceof SQLVariantRefExpr) {
                return false;
            }

            limit.setRowCount(new SQLIntegerExpr(count));
        }

        if (limit == null) {
            limit = new SQLLimit();
            if (offset > 0) {
                limit.setOffset(new SQLIntegerExpr(offset));
            }
            limit.setRowCount(new SQLIntegerExpr(count));
            queryBlock.setLimit(limit);
        }

        return true;
    }

    private static boolean limitQueryBlock(SQLSelect select, DbType dbType, int offset, int count, boolean check) {
        SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) select.getQuery();
        return limitMySqlQueryBlock(queryBlock, dbType, offset, count, check);
    }

    private static boolean limitSQLQueryBlock(SQLSelectQueryBlock queryBlock, DbType dbType, int offset, int count,
                                              boolean check) {
        SQLLimit limit = queryBlock.getLimit();
        if (limit != null) {
            if (offset > 0) {
                limit.setOffset(new SQLIntegerExpr(offset));
            }

            if (check && limit.getRowCount() instanceof SQLNumericLiteralExpr) {
                int rowCount = ((SQLNumericLiteralExpr) limit.getRowCount()).getNumber().intValue();
                if (rowCount <= count && offset <= 0) {
                    return false;
                }
            }

            limit.setRowCount(new SQLIntegerExpr(count));
        }

        limit = new SQLLimit();
        if (offset > 0) {
            limit.setOffset(new SQLIntegerExpr(offset));
        }
        limit.setRowCount(new SQLIntegerExpr(count));
        queryBlock.setLimit(limit);
        return true;
    }

    private static boolean limitMySqlQueryBlock(SQLSelectQueryBlock queryBlock, DbType dbType, int offset, int count,
                                                boolean check) {
        SQLLimit limit = queryBlock.getLimit();
        if (limit != null) {
            if (offset > 0) {
                limit.setOffset(new SQLIntegerExpr(offset));
            }

            if (check && limit.getRowCount() instanceof SQLNumericLiteralExpr) {
                int rowCount = ((SQLNumericLiteralExpr) limit.getRowCount()).getNumber().intValue();
                if (rowCount <= count && offset <= 0) {
                    return false;
                }
            } else if (check && limit.getRowCount() instanceof SQLVariantRefExpr) {
                return false;
            }

            limit.setRowCount(new SQLIntegerExpr(count));
        }

        if (limit == null) {
            limit = new SQLLimit();
            if (offset > 0) {
                limit.setOffset(new SQLIntegerExpr(offset));
            }
            limit.setRowCount(new SQLIntegerExpr(count));
            queryBlock.setLimit(limit);
        }

        return true;
    }

    private static String count(SQLSelect select, DbType dbType) {
        if (select.getOrderBy() != null) {
            select.setOrderBy(null);
        }

        SQLSelectQuery query = select.getQuery();
        clearOrderBy(query);

        if (query instanceof SQLSelectQueryBlock) {
            SQLSelectItem countItem = createCountItem(dbType);

            SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) query;
            List<SQLSelectItem> selectList = queryBlock.getSelectList();

            if (queryBlock.getGroupBy() != null
                && queryBlock.getGroupBy().getItems().size() > 0) {
                if (queryBlock.getSelectList().size() == 1
                    && queryBlock.getSelectList().get(0).getExpr() instanceof SQLAllColumnExpr
                ) {
                    queryBlock.getSelectList().clear();
                    queryBlock.getSelectList().add(new SQLSelectItem(new SQLIntegerExpr(1)));
                }
                return createCountUseSubQuery(select, dbType);
            }

            int option = queryBlock.getDistionOption();
            if (option == SQLSetQuantifier.DISTINCT
                && selectList.size() >= 1) {
                SQLAggregateExpr countExpr = new SQLAggregateExpr("COUNT", SQLAggregateOption.DISTINCT);
                for (int i = 0; i < selectList.size(); ++i) {
                    countExpr.addArgument(selectList.get(i).getExpr());
                }
                selectList.clear();
                queryBlock.setDistionOption(0);
                queryBlock.addSelectItem(countExpr);
            } else {
                selectList.clear();
                selectList.add(countItem);
            }
            return SQLUtils.toSQLString(select, dbType);
        } else if (query instanceof SQLUnionQuery) {
            return createCountUseSubQuery(select, dbType);
        }

        throw new IllegalStateException();
    }

    private static String createCountUseSubQuery(SQLSelect select, DbType dbType) {
        SQLSelectQueryBlock countSelectQuery = createQueryBlock(dbType);

        SQLSelectItem countItem = createCountItem(dbType);
        countSelectQuery.getSelectList().add(countItem);

        SQLSubqueryTableSource fromSubquery = new SQLSubqueryTableSource(select);
        fromSubquery.setAlias("ALIAS_COUNT");
        countSelectQuery.setFrom(fromSubquery);

        SQLSelect countSelect = new SQLSelect(countSelectQuery);
        SQLSelectStatement countStmt = new SQLSelectStatement(countSelect, dbType);

        return SQLUtils.toSQLString(countStmt, dbType);
    }

    private static SQLSelectQueryBlock createQueryBlock(DbType dbType) {
        return new MySqlSelectQueryBlock();
    }

    private static SQLSelectItem createCountItem(DbType dbType) {
        SQLAggregateExpr countExpr = new SQLAggregateExpr("COUNT");

        countExpr.addArgument(new SQLAllColumnExpr());

        SQLSelectItem countItem = new SQLSelectItem(countExpr);
        return countItem;
    }

    private static void clearOrderBy(SQLSelectQuery query) {
        if (query instanceof SQLSelectQueryBlock) {
            SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) query;
            if (queryBlock.getOrderBy() != null) {
                queryBlock.setOrderBy(null);
            }
            return;
        }

        if (query instanceof SQLUnionQuery) {
            SQLUnionQuery union = (SQLUnionQuery) query;
            if (union.getOrderBy() != null) {
                union.setOrderBy(null);
            }
            clearOrderBy(union.getLeft());
            clearOrderBy(union.getRight());
        }
    }

    /**
     * @return if not exists limit, return -1;
     */
    public static int getLimit(String sql, DbType dbType) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        if (stmtList.size() != 1) {
            return -1;
        }

        SQLStatement stmt = stmtList.get(0);

        if (stmt instanceof SQLSelectStatement) {
            SQLSelectStatement selectStmt = (SQLSelectStatement) stmt;
            SQLSelectQuery query = selectStmt.getSelect().getQuery();
            if (query instanceof SQLSelectQueryBlock) {
                if (query instanceof MySqlSelectQueryBlock) {
                    SQLLimit limit = ((MySqlSelectQueryBlock) query).getLimit();

                    if (limit == null) {
                        return -1;
                    }

                    SQLExpr rowCountExpr = limit.getRowCount();

                    if (rowCountExpr instanceof SQLNumericLiteralExpr) {
                        int rowCount = ((SQLNumericLiteralExpr) rowCountExpr).getNumber().intValue();
                        return rowCount;
                    }

                    return Integer.MAX_VALUE;
                }
                return -1;
            }
        }

        return -1;
    }

    public static boolean hasUnorderedLimit(String sql, DbType dbType) {
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        if (DbType.mysql == dbType) {

            MySqlUnorderedLimitDetectVisitor visitor = new MySqlUnorderedLimitDetectVisitor();

            for (SQLStatement stmt : stmtList) {
                stmt.accept(visitor);
            }

            return visitor.unorderedLimitCount > 0;
        }

        throw new FastsqlException("not supported. dbType : " + dbType);
    }

    private static class MySqlUnorderedLimitDetectVisitor extends MySqlASTVisitorAdapter {
        public int unorderedLimitCount;

        @Override
        public boolean visit(MySqlSelectQueryBlock x) {
            SQLOrderBy orderBy = x.getOrderBy();
            SQLLimit limit = x.getLimit();

            if (limit != null && (orderBy == null || orderBy.getItems().size() == 0)) {
                boolean subQueryHasOrderBy = false;
                SQLTableSource from = x.getFrom();
                if (from instanceof SQLSubqueryTableSource) {
                    SQLSubqueryTableSource subqueryTabSrc = (SQLSubqueryTableSource) from;
                    SQLSelect select = subqueryTabSrc.getSelect();
                    if (select.getQuery() instanceof SQLSelectQueryBlock) {
                        SQLSelectQueryBlock subquery = (SQLSelectQueryBlock) select.getQuery();
                        if (subquery.getOrderBy() != null && subquery.getOrderBy().getItems().size() > 0) {
                            subQueryHasOrderBy = true;
                        }
                    }
                }

                if (!subQueryHasOrderBy) {
                    unorderedLimitCount++;
                }
            }
            return true;
        }
    }

}
