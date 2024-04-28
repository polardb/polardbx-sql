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
package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLDeclareItem;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.ast.SQLOrderBy;
import com.alibaba.polardbx.druid.sql.ast.SQLOver;
import com.alibaba.polardbx.druid.sql.ast.SQLParameter;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.SQLWindow;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBetweenExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCastExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLExprUtils;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLInListExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLBlockStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateProcedureStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDeleteStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExplainStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLIfStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLInsertStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelect;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQuery;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnionOperator;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnionQuery;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUnionQueryTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLValuesTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLWithSubqueryClause;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.polardbx.druid.util.FnvHash;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.StringType;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PolarXBindParamTypeVisitor extends SQLExprTypeVisitor {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    protected final HashMap<Name, TableMeta> tableMetas = new LinkedHashMap<>();

    protected final HashMap<SQLVariantRefExpr, ColumnMeta> dynamicParamBindColumn = new LinkedHashMap<>();

    protected final HashMap<SQLObject, ColumnMeta> columnMaps = new LinkedHashMap<>();

    private String defaultEncoding = CharsetName.UTF8.name();

    private final ColumnMeta virtualColumn;

    public PolarXBindParamTypeVisitor(ExecutionContext context) {
        super(context);
        if (StringUtils.isNotEmpty(context.getEncoding())) {
            this.defaultEncoding = context.getEncoding();
        }
        Field field =
            new Field(new StringType(CharsetName.of(defaultEncoding, true), CollationName.defaultCollation()));
        this.virtualColumn = new ColumnMeta("", "", "", field);
    }

    public Map<SQLVariantRefExpr, ColumnMeta> getDynamicParamBindColumn() {
        return dynamicParamBindColumn;
    }

    public TableMeta getTableStat(SQLName tableName) {
        String schemaName = null;
        String strName;
        if (tableName instanceof SQLIdentifierExpr) {
            strName = ((SQLIdentifierExpr) tableName).normalizedName();
        } else if (tableName instanceof SQLPropertyExpr) {
            strName = SQLUtils.normalize(tableName.getSimpleName());
            schemaName = SQLUtils.normalize(((SQLPropertyExpr) tableName).getOwnerName());
        } else {
            strName = SQLUtils.normalize(tableName.toString());
        }
        if (StringUtils.isBlank(schemaName)) {
            schemaName = DefaultSchema.getSchemaName();
        }
        return getTableStat(schemaName, strName);
    }

    private TableMeta getTableStat(String schema, String tableName) {
        String realSchema = StringUtils.isBlank(schema) ? DefaultSchema.getSchemaName() : schema;
        String realTableName = handleName(tableName);
        Name tableNameObj = new Name(realSchema, realTableName);
        TableMeta stat = tableMetas.get(tableNameObj);
        if (stat == null) {
            try {
                RelOptTable sourceTable = catalogReader.getTableForMember(
                    ImmutableList.of(realSchema, realTableName));
                if (sourceTable != null) {
                    TableMeta tableMeta = CBOUtil.getTableMeta(sourceTable);
                    tableMetas.put(tableNameObj, tableMeta);
                    stat = tableMeta;
                }
            } catch (Throwable t) {
                //ignore, allow get table failure!
            }
        }
        return stat;
    }

    private TableMeta getTableStat(String tableName) {
        String schema = DefaultSchema.getSchemaName();
        if (tableName.indexOf('.') != -1) {
            schema = tableName.substring(0, tableName.indexOf('.'));
            schema = SQLUtils.normalize(schema);
            tableName = tableName.substring(tableName.indexOf('.') + 1, tableName.length());
            tableName = SQLUtils.normalize(tableName);
        }
        return getTableStat(schema, tableName);
    }

    private ColumnMeta getColumnStat(String schema, String tableName, String columnName) {
        String realTableName = handleName(tableName);
        String realColumnName = handleName(columnName);
        TableMeta tableMeta = getTableStat(schema, realTableName);
        if (tableMeta != null) {
            return tableMeta.getColumn(realColumnName);
        }
        return null;
    }

    protected ColumnMeta getColumnStat(SQLName table, String columnName) {
        String schemaName = null;
        String tableName;
        if (table instanceof SQLIdentifierExpr) {
            tableName = ((SQLIdentifierExpr) table).normalizedName();
        } else if (table instanceof SQLPropertyExpr) {
            tableName = SQLUtils.normalize(table.getSimpleName());
            schemaName = SQLUtils.normalize(((SQLPropertyExpr) table).getOwnerName());
        } else {
            tableName = SQLUtils.normalize(table.toString());
        }
        if (StringUtils.isBlank(schemaName)) {
            schemaName = DefaultSchema.getSchemaName();
        }

        return getColumnStat(schemaName, tableName, columnName);
    }

    protected ColumnMeta getColumnStat(String tableName, String columnName) {
        String schema = DefaultSchema.getSchemaName();
        if (tableName.indexOf('.') != -1) {
            schema = SQLUtils.normalize(tableName.substring(0, tableName.indexOf('.')));
            tableName = SQLUtils.normalize(tableName.substring(tableName.indexOf('.') + 1, tableName.length()));
        }
        return getColumnStat(schema, tableName, columnName);
    }

    private String handleName(String ident) {
        int len = ident.length();
        if (ident.charAt(0) == '[' && ident.charAt(len - 1) == ']') {
            ident = ident.substring(1, len - 1);
        } else {
            boolean flag0 = false;
            boolean flag1 = false;
            boolean flag2 = false;
            boolean flag3 = false;
            for (int i = 0; i < len; ++i) {
                final char ch = ident.charAt(i);
                if (ch == '\"') {
                    flag0 = true;
                } else if (ch == '`') {
                    flag1 = true;
                } else if (ch == ' ') {
                    flag2 = true;
                } else if (ch == '\'') {
                    flag3 = true;
                }
            }
            if (flag0) {
                ident = ident.replaceAll("\"", "");
            }

            if (flag1) {
                ident = ident.replaceAll("`", "");
            }

            if (flag2) {
                ident = ident.replaceAll(" ", "");
            }

            if (flag3) {
                ident = ident.replaceAll("'", "");
            }
        }
        return ident;
    }

    @Override
    public boolean visit(SQLOver x) {
        SQLName of = x.getOf();
        SQLOrderBy orderBy = x.getOrderBy();
        List<SQLExpr> partitionBy = x.getPartitionBy();

        if (of == null // skip if of is not null
            && orderBy != null) {
            orderBy.accept(this);
        }

        if (partitionBy != null) {
            for (SQLExpr expr : partitionBy) {
                expr.accept(this);
            }
        }

        return false;
    }

    @Override
    public boolean visit(SQLBetweenExpr x) {
        SQLExpr test = x.getTestExpr();
        SQLExpr begin = x.getBeginExpr();
        SQLExpr end = x.getEndExpr();
        statExpr(test);
        statExpr(begin);
        statExpr(end);
        handleValueExprs(test, begin, end);
        return false;
    }

    @Override
    public boolean visit(SQLBinaryOpExpr x) {
        SQLObject parent = x.getParent();

        if (parent instanceof SQLIfStatement) {
            return true;
        }

        final SQLBinaryOperator op = x.getOperator();
        final SQLExpr left = x.getLeft();
        final SQLExpr right = x.getRight();

        if ((op == SQLBinaryOperator.BooleanAnd || op == SQLBinaryOperator.BooleanOr)
            && left instanceof SQLBinaryOpExpr
            && ((SQLBinaryOpExpr) left).getOperator() == op) {
            List<SQLExpr> groupList = SQLBinaryOpExpr.split(x, op);
            for (int i = 0; i < groupList.size(); i++) {
                SQLExpr item = groupList.get(i);
                item.accept(this);
            }
            return false;
        }

        switch (op) {
        case Equality:
        case NotEqual:
        case GreaterThan:
        case GreaterThanOrEqual:
        case LessThan:
        case LessThanOrGreater:
        case LessThanOrEqual:
        case LessThanOrEqualOrGreaterThan:
        case SoudsLike:
        case Like:
        case NotLike:
        case Is:
        case IsNot:
            handleValueExprs(left, right);
            handleValueExprs(right, left);
            break;
        case BooleanOr: {
            List<SQLExpr> list = SQLBinaryOpExpr.split(x, op);

            for (SQLExpr item : list) {
                if (item instanceof SQLBinaryOpExpr) {
                    visit((SQLBinaryOpExpr) item);
                } else {
                    item.accept(this);
                }
            }

            return false;
        }
        case Modulus:
            if (right instanceof SQLIdentifierExpr) {
                long hashCode64 = ((SQLIdentifierExpr) right).hashCode64();
                if (hashCode64 == FnvHash.Constants.ISOPEN) {
                    left.accept(this);
                    return false;
                }
            }
            break;
        default:
            break;
        }
        statExpr(left);
        statExpr(right);
        return false;
    }

    protected ColumnMeta getColumn(SQLExpr expr) {
        SQLExpr orig = expr;
        ColumnMeta ret = columnMaps.get(orig);
        if (ret != null) {
            return ret;
        }
        // unwrap
        expr = unwrapExpr(expr);
        if (expr instanceof SQLPropertyExpr) {
            SQLPropertyExpr propertyExpr = (SQLPropertyExpr) expr;

            SQLExpr owner = propertyExpr.getOwner();
            String column = SQLUtils.normalize(propertyExpr.getName());

            if (owner instanceof SQLName) {
                SQLName table = (SQLName) owner;

                SQLObject resolvedOwnerObject = propertyExpr.getResolvedOwnerObject();
                if (resolvedOwnerObject instanceof SQLSubqueryTableSource
                    || resolvedOwnerObject instanceof SQLCreateProcedureStatement
                    || resolvedOwnerObject instanceof SQLCreateFunctionStatement) {
                    table = null;
                }

                if (resolvedOwnerObject instanceof SQLExprTableSource) {
                    SQLExpr tableSourceExpr = ((SQLExprTableSource) resolvedOwnerObject).getExpr();
                    if (tableSourceExpr instanceof SQLName) {
                        table = (SQLName) tableSourceExpr;
                    }
                } else if (resolvedOwnerObject instanceof SQLValuesTableSource) {
                    return null;
                }

                if (table != null) {
                    ret = getColumnStat(table, column);
                    if (ret != null) {
                        columnMaps.put(orig, ret);
                    }
                    return ret;
                }
            }
            return null;
        }

        if (expr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr identifierExpr = (SQLIdentifierExpr) expr;
            if (identifierExpr.getResolvedParameter() != null) {
                return null;
            }

            if (identifierExpr.getResolvedTableSource() instanceof SQLSubqueryTableSource) {
                return null;
            }

            if (identifierExpr.getResolvedDeclareItem() != null || identifierExpr.getResolvedParameter() != null) {
                return null;
            }

            String column = identifierExpr.getName();

            SQLName table = null;
            SQLTableSource tableSource = identifierExpr.getResolvedTableSource();
            if (tableSource instanceof SQLExprTableSource) {
                SQLExpr tableSourceExpr = ((SQLExprTableSource) tableSource).getExpr();

                if (tableSourceExpr != null && !(tableSourceExpr instanceof SQLName)) {
                    tableSourceExpr = unwrapExpr(tableSourceExpr);
                }

                if (tableSourceExpr instanceof SQLName) {
                    table = (SQLName) tableSourceExpr;
                }
            }

            if (table != null) {
                ret = getColumnStat(table, column);
                if (ret != null) {
                    columnMaps.put(orig, ret);
                }
                return ret;
            }
            return null;
        }

        if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr methodInvokeExpr = (SQLMethodInvokeExpr) expr;
            List<SQLExpr> arguments = methodInvokeExpr.getArguments();
            long nameHash = methodInvokeExpr.methodNameHashCode64();
            if (nameHash == FnvHash.Constants.DATE_FORMAT) {
                if (arguments.size() == 2
                    && arguments.get(0) instanceof SQLName
                    && arguments.get(1) instanceof SQLCharExpr) {
                    ret = getColumn(arguments.get(0));
                    if (ret != null) {
                        columnMaps.put(orig, ret);
                    }
                    return ret;
                }
            }
        }
        return null;
    }

    private SQLExpr unwrapExpr(SQLExpr expr) {
        SQLExpr original = expr;
        for (int i = 0; ; i++) {
            if (i > 100) {
                logger.warn("exceed than 100 iteration! " + expr);
                return null;
            }

            if (expr instanceof SQLMethodInvokeExpr) {
                SQLMethodInvokeExpr methodInvokeExp = (SQLMethodInvokeExpr) expr;
                if (methodInvokeExp.getArguments().size() == 1) {
                    SQLExpr firstExpr = methodInvokeExp.getArguments().get(0);
                    expr = firstExpr;
                    continue;
                }
            }

            if (expr instanceof SQLCastExpr) {
                expr = ((SQLCastExpr) expr).getExpr();
                continue;
            }

            if (expr instanceof SQLPropertyExpr) {
                SQLPropertyExpr propertyExpr = (SQLPropertyExpr) expr;

                SQLTableSource resolvedTableSource = propertyExpr.getResolvedTableSource();
                if (resolvedTableSource instanceof SQLSubqueryTableSource) {
                    SQLSelect select = ((SQLSubqueryTableSource) resolvedTableSource).getSelect();
                    SQLSelectQueryBlock queryBlock = select.getFirstQueryBlock();
                    if (queryBlock != null) {
                        if (queryBlock.getGroupBy() != null) {
                            if (original.getParent() instanceof SQLBinaryOpExpr) {
                                SQLExpr other = ((SQLBinaryOpExpr) original.getParent()).other(original);
                                if (!SQLExprUtils.isLiteralExpr(other)) {
                                    break;
                                }
                            }
                        }

                        SQLSelectItem selectItem = queryBlock.findSelectItem(propertyExpr
                            .nameHashCode64());
                        if (selectItem != null) {
                            SQLExpr selectItemExpr = selectItem.getExpr();
                            if (selectItemExpr instanceof SQLMethodInvokeExpr
                                && ((SQLMethodInvokeExpr) selectItemExpr).getArguments().size() == 1
                            ) {
                                selectItemExpr = ((SQLMethodInvokeExpr) selectItemExpr).getArguments().get(0);
                            }
                            if (selectItemExpr != expr) {
                                expr = selectItemExpr;
                                continue;
                            }
                        } else if (queryBlock.selectItemHasAllColumn()) {
                            SQLTableSource allColumnTableSource = null;

                            SQLTableSource from = queryBlock.getFrom();
                            if (from instanceof SQLJoinTableSource) {
                                SQLSelectItem allColumnSelectItem = queryBlock.findAllColumnSelectItem();
                                if (allColumnSelectItem != null
                                    && allColumnSelectItem.getExpr() instanceof SQLPropertyExpr) {
                                    SQLExpr owner = ((SQLPropertyExpr) allColumnSelectItem.getExpr()).getOwner();
                                    if (owner instanceof SQLName) {
                                        allColumnTableSource = from.findTableSource(((SQLName) owner).nameHashCode64());
                                    }
                                }
                            } else {
                                allColumnTableSource = from;
                            }

                            if (allColumnTableSource == null) {
                                break;
                            }

                            propertyExpr = propertyExpr.clone();
                            propertyExpr.setResolvedTableSource(allColumnTableSource);

                            if (allColumnTableSource instanceof SQLExprTableSource) {
                                propertyExpr.setOwner(((SQLExprTableSource) allColumnTableSource).getExpr().clone());
                            }
                            expr = propertyExpr;
                            continue;
                        }
                    }
                } else if (resolvedTableSource instanceof SQLExprTableSource) {
                    SQLExprTableSource exprTableSource = (SQLExprTableSource) resolvedTableSource;
                    if (exprTableSource.getSchemaObject() != null) {
                        break;
                    }

                    SQLTableSource redirectTableSource = null;
                    SQLExpr tableSourceExpr = exprTableSource.getExpr();
                    if (tableSourceExpr instanceof SQLIdentifierExpr) {
                        redirectTableSource = ((SQLIdentifierExpr) tableSourceExpr).getResolvedTableSource();
                    } else if (tableSourceExpr instanceof SQLPropertyExpr) {
                        redirectTableSource = ((SQLPropertyExpr) tableSourceExpr).getResolvedTableSource();
                    }

                    if (redirectTableSource == resolvedTableSource) {
                        redirectTableSource = null;
                    }

                    if (redirectTableSource != null) {
                        propertyExpr = propertyExpr.clone();
                        if (redirectTableSource instanceof SQLExprTableSource) {
                            propertyExpr.setOwner(((SQLExprTableSource) redirectTableSource).getExpr().clone());
                        }
                        propertyExpr.setResolvedTableSource(redirectTableSource);
                        expr = propertyExpr;
                        continue;
                    }

                    propertyExpr = propertyExpr.clone();
                    propertyExpr.setOwner(tableSourceExpr);
                    expr = propertyExpr;
                    break;
                }
            }
            break;
        }

        return expr;
    }

    @Override
    public boolean visit(MySqlInsertStatement x) {
        TableMeta tableMeta = getTableStat(x.getTableSource());

        x.getTableSource().getColumns();
        accept(x.getColumns());
        accept(x.getValuesList());
        handleInsertValue(tableMeta, x.getColumns(), x.getValuesList());
        accept(x.getQuery());
        accept(x.getDuplicateKeyUpdate());

        return false;
    }

    @Override
    public boolean visit(SQLInsertStatement x) {
        TableMeta tableMeta = null;
        if (x.getTableName() instanceof SQLName) {
            tableMeta = getTableStat(x.getTableName());
        }
        accept(x.getColumns());
        accept(x.getValuesList());
        handleInsertValue(tableMeta, x.getColumns(), x.getValuesList());
        accept(x.getQuery());

        return false;
    }

    protected void handleInsertValue(TableMeta tableMeta,
                                     List<SQLExpr> exprs, List<SQLInsertStatement.ValuesClause> valueExprs) {
        for (int i = 0; i < valueExprs.size(); i++) {
            SQLInsertStatement.ValuesClause valuesClause = valueExprs.get(i);
            handleValueExpr(tableMeta, exprs, valuesClause.getValues());
        }
    }

    protected void handleValueExpr(TableMeta tableMeta, List<SQLExpr> exprs, List<SQLExpr> valueExprs) {
        if (exprs.size() == valueExprs.size() || (
            exprs.size() == 0 && tableMeta.getAllColumns().size() == valueExprs.size())) {
            for (int i = 0; i < valueExprs.size(); i++) {
                ColumnMeta column = null;
                if (exprs.size() == 0) {
                    column = tableMeta.getAllColumns().get(i);
                } else {
                    SQLExpr expr = exprs.get(i);
                    if (expr instanceof SQLCastExpr) {
                        expr = ((SQLCastExpr) expr).getExpr();
                    }
                    column = getColumn(expr);
                }

                if (column == null) {
                    return;
                }
                SQLExpr valueExpr = valueExprs.get(i);
                if (valueExpr instanceof SQLVariantRefExpr) {
                    SQLVariantRefExpr item = (SQLVariantRefExpr) valueExpr;
                    if (dynamicParamBindColumn.containsKey(item)) {
                        continue;
                    }
                    if (item instanceof SQLVariantRefExpr) {
                        SQLVariantRefExpr variantRefExpr = ((SQLVariantRefExpr) item);
                        if (variantRefExpr.isGlobal() || variantRefExpr.isSession()) {
                            //ignore
                        } else if ("?".equalsIgnoreCase(variantRefExpr.getName())) {
                            dynamicParamBindColumn.put(variantRefExpr, column);
                        } else {
                            //ignore
                        }
                    }
                }
            }
        }
    }

    protected void accept(List<? extends SQLObject> nodes) {
        for (int i = 0, size = nodes.size(); i < size; ++i) {
            accept(nodes.get(i));
        }
    }

    @Override
    public boolean visit(MySqlSelectQueryBlock x) {
        return visit((SQLSelectQueryBlock) x);
    }

    @Override
    public boolean visit(SQLSelectQueryBlock x) {
        SQLTableSource from = x.getFrom();

        if (from == null) {
            for (SQLSelectItem selectItem : x.getSelectList()) {
                SQLExpr sqlExpr = selectItem.getExpr();
                if (sqlExpr instanceof SQLVariantRefExpr) {
                    handleSelectItemVariantRefExpr((SQLVariantRefExpr) sqlExpr);
                } else {
                    statExpr(
                        selectItem.getExpr());
                }
            }
            return false;
        }

        if (from != null) {
            // 提前执行，获得aliasMap
            from.accept(this);
        }

        SQLExprTableSource into = x.getInto();
        if (into != null && into.getExpr() instanceof SQLName) {
            SQLName intoExpr = (SQLName) into.getExpr();
            boolean isParam = intoExpr instanceof SQLIdentifierExpr && isParam((SQLIdentifierExpr) intoExpr);
            if (!isParam) {
                getTableStat(intoExpr);
            }
            into.accept(this);
        }

        for (SQLSelectItem selectItem : x.getSelectList()) {
            if (selectItem.getClass() == SQLSelectItem.class) {
                statExpr(
                    selectItem.getExpr());
            } else {
                selectItem.accept(this);
            }
        }

        SQLExpr where = x.getWhere();
        if (where != null) {
            statExpr(where);
        }

        SQLExpr startWith = x.getStartWith();
        if (startWith != null) {
            statExpr(startWith);
        }

        SQLExpr connectBy = x.getConnectBy();
        if (connectBy != null) {
            statExpr(connectBy);
        }

        SQLSelectGroupByClause groupBy = x.getGroupBy();
        if (groupBy != null) {
            for (SQLExpr expr : groupBy.getItems()) {
                statExpr(expr);
            }
        }

        List<SQLWindow> windows = x.getWindows();
        if (windows != null && windows.size() > 0) {
            for (SQLWindow window : windows) {
                window.accept(this);
            }
        }

        SQLOrderBy orderBy = x.getOrderBy();
        if (orderBy != null) {
            this.visit(orderBy);
        }

        SQLExpr first = x.getFirst();
        if (first != null) {
            statExpr(first);
        }

        List<SQLSelectOrderByItem> distributeBy = x.getDistributeBy();
        if (distributeBy != null) {
            for (SQLSelectOrderByItem item : distributeBy) {
                statExpr(item.getExpr());
            }
        }

        List<SQLSelectOrderByItem> sortBy = x.getSortBy();
        if (sortBy != null) {
            for (SQLSelectOrderByItem orderByItem : sortBy) {
                statExpr(orderByItem.getExpr());
            }
        }

        for (SQLExpr expr : x.getForUpdateOf()) {
            statExpr(expr);
        }

        return false;
    }

    private static boolean isParam(SQLIdentifierExpr x) {
        if (x.getResolvedParameter() != null
            || x.getResolvedDeclareItem() != null) {
            return true;
        }
        return false;
    }

    @Override
    public boolean visit(SQLJoinTableSource x) {
        SQLTableSource left = x.getLeft(), right = x.getRight();

        left.accept(this);
        right.accept(this);

        SQLExpr condition = x.getCondition();
        if (condition != null) {
            condition.accept(this);
        }

        if (x.getUsing().size() > 0
            && left instanceof SQLExprTableSource && right instanceof SQLExprTableSource) {
            SQLExpr leftExpr = ((SQLExprTableSource) left).getExpr();
            SQLExpr rightExpr = ((SQLExprTableSource) right).getExpr();

            for (SQLExpr expr : x.getUsing()) {
                if (expr instanceof SQLIdentifierExpr) {
                    String name = ((SQLIdentifierExpr) expr).getName();
                    SQLPropertyExpr leftPropExpr = new SQLPropertyExpr(leftExpr, name);
                    SQLPropertyExpr rightPropExpr = new SQLPropertyExpr(rightExpr, name);

                    leftPropExpr.setResolvedTableSource(left);
                    rightPropExpr.setResolvedTableSource(right);

                    SQLBinaryOpExpr usingCondition =
                        new SQLBinaryOpExpr(leftPropExpr, SQLBinaryOperator.Equality, rightPropExpr);
                    usingCondition.accept(this);
                }
            }
        }

        return false;
    }

    @Override
    public boolean visit(SQLSelectStatement x) {
        visit(x.getSelect());
        return false;
    }

    @Override
    public boolean visit(SQLWithSubqueryClause.Entry x) {
        SQLWithSubqueryClause with = (SQLWithSubqueryClause) x.getParent();
        if (Boolean.TRUE == with.getRecursive()) {
            SQLSelect select = x.getSubQuery();
            if (select != null) {
                select.accept(this);
            } else {
                x.getReturningStatement().accept(this);
            }
        } else {
            SQLSelect select = x.getSubQuery();
            if (select != null) {
                select.accept(this);
            } else {
                x.getReturningStatement().accept(this);
            }
        }

        return false;
    }

    @Override
    public boolean visit(SQLSubqueryTableSource x) {
        x.getSelect().accept(this);
        return false;
    }

    protected boolean isSimpleExprTableSource(SQLExprTableSource x) {
        return x.getExpr() instanceof SQLName;
    }

    public TableMeta getTableStat(SQLExprTableSource tableSource) {
        return getTableStatWithUnwrap(
            tableSource.getExpr());
    }

    protected TableMeta getTableStatWithUnwrap(SQLExpr expr) {
        SQLExpr identExpr = null;

        expr = unwrapExpr(expr);

        if (expr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr identifierExpr = (SQLIdentifierExpr) expr;

            if (identifierExpr.nameHashCode64() == FnvHash.Constants.DUAL) {
                return null;
            }

            if (isSubQueryOrParamOrVariant(identifierExpr)) {
                return null;
            }
        }

        SQLTableSource tableSource = null;
        if (expr instanceof SQLIdentifierExpr) {
            tableSource = ((SQLIdentifierExpr) expr).getResolvedTableSource();
        } else if (expr instanceof SQLPropertyExpr) {
            tableSource = ((SQLPropertyExpr) expr).getResolvedTableSource();
        }

        if (tableSource instanceof SQLExprTableSource) {
            SQLExpr tableSourceExpr = ((SQLExprTableSource) tableSource).getExpr();
            if (tableSourceExpr instanceof SQLName) {
                identExpr = tableSourceExpr;
            }
        }

        if (identExpr == null) {
            identExpr = expr;
        }

        if (identExpr instanceof SQLName) {
            return getTableStat((SQLName) identExpr);
        }
        return getTableStat(identExpr.toString());
    }

    @Override
    public boolean visit(SQLExprTableSource x) {
        SQLExpr expr = x.getExpr();
        if (expr instanceof SQLMethodInvokeExpr) {
            SQLMethodInvokeExpr func = (SQLMethodInvokeExpr) expr;
            if (func.methodNameHashCode64() == FnvHash.Constants.ANN) {
                expr = func.getArguments().get(0);
            }
        }

        if (isSimpleExprTableSource(x)) {
            getTableStatWithUnwrap(expr);
        } else {
            accept(expr);
        }
        return false;
    }

    protected boolean isSubQueryOrParamOrVariant(SQLIdentifierExpr identifierExpr) {
        SQLObject resolvedColumnObject = identifierExpr.getResolvedColumnObject();
        if (resolvedColumnObject instanceof SQLWithSubqueryClause.Entry
            || resolvedColumnObject instanceof SQLParameter
            || resolvedColumnObject instanceof SQLDeclareItem) {
            return true;
        }

        SQLObject resolvedOwnerObject = identifierExpr.getResolvedOwnerObject();
        if (resolvedOwnerObject instanceof SQLSubqueryTableSource
            || resolvedOwnerObject instanceof SQLWithSubqueryClause.Entry) {
            return true;
        }

        return false;
    }

    @Override
    public boolean visit(SQLSelectItem x) {
        statExpr(x.getExpr());
        return false;
    }

    @Override
    public boolean visit(SQLSelect x) {
        SQLWithSubqueryClause with = x.getWithSubQuery();
        if (with != null) {
            with.accept(this);
        }

        SQLSelectQuery query = x.getQuery();
        if (query != null) {
            query.accept(this);
        }

        SQLOrderBy orderBy = x.getOrderBy();
        if (orderBy != null) {
            accept(x.getOrderBy());
        }
        return false;
    }

    @Override
    public boolean visit(SQLAggregateExpr x) {
        accept(x.getArguments());
        accept(x.getOrderBy());
        accept(x.getOver());
        return false;
    }

    @Override
    public boolean visit(SQLMethodInvokeExpr x) {
        accept(x.getArguments());
        return false;
    }

    @Override
    public boolean visit(MySqlUpdateStatement x) {
        return visit((SQLUpdateStatement) x);
    }

    @Override
    public boolean visit(SQLUpdateStatement x) {
        SQLTableSource tableSource = x.getTableSource();
        if (tableSource instanceof SQLExprTableSource) {
            SQLName identName = ((SQLExprTableSource) tableSource).getName();
            getTableStat(identName);
        } else {
            tableSource.accept(this);
        }

        final SQLTableSource from = x.getFrom();
        if (from != null) {
            from.accept(this);
        }

        final List<SQLUpdateSetItem> items = x.getItems();
        for (int i = 0, size = items.size(); i < size; ++i) {
            SQLUpdateSetItem item = items.get(i);
            visit(item);
        }

        final SQLExpr where = x.getWhere();
        if (where != null) {
            where.accept(this);
        }

        return false;
    }

    @Override
    public boolean visit(SQLUpdateSetItem x) {
        final SQLExpr column = x.getColumn();
        final SQLExpr value = x.getValue();
        handleValueExprs(column, value);
        if (value != null) {
            statExpr(value);
        }
        return false;
    }

    @Override
    public boolean visit(MySqlDeleteStatement x) {

        SQLTableSource from = x.getFrom();
        if (from != null) {
            from.accept(this);
        }

        SQLTableSource using = x.getUsing();
        if (using != null) {
            using.accept(this);
        }

        SQLTableSource tableSource = x.getTableSource();
        tableSource.accept(this);

        if (tableSource instanceof SQLExprTableSource) {
            getTableStat((SQLExprTableSource) tableSource);
        }

        accept(x.getWhere());

        accept(x.getOrderBy());
        accept(x.getLimit());

        return false;
    }

    @Override
    public boolean visit(SQLUnionQuery x) {
        SQLUnionOperator operator = x.getOperator();
        List<SQLSelectQuery> relations = x.getRelations();
        if (relations.size() > 2) {
            for (SQLSelectQuery relation : x.getRelations()) {
                relation.accept(this);
            }
            return false;
        }

        SQLSelectQuery left = x.getLeft();
        SQLSelectQuery right = x.getRight();

        boolean bracket = x.isBracket() && !(x.getParent() instanceof SQLUnionQueryTableSource);

        if ((!bracket)
            && left instanceof SQLUnionQuery
            && ((SQLUnionQuery) left).getOperator() == operator
            && !right.isBracket()
            && x.getOrderBy() == null) {

            SQLUnionQuery leftUnion = (SQLUnionQuery) left;

            List<SQLSelectQuery> rights = new ArrayList<SQLSelectQuery>();
            rights.add(right);

            for (; ; ) {
                SQLSelectQuery leftLeft = leftUnion.getLeft();
                SQLSelectQuery leftRight = leftUnion.getRight();

                if ((!leftUnion.isBracket())
                    && leftUnion.getOrderBy() == null
                    && (!leftLeft.isBracket())
                    && (!leftRight.isBracket())
                    && leftLeft instanceof SQLUnionQuery
                    && ((SQLUnionQuery) leftLeft).getOperator() == operator) {
                    rights.add(leftRight);
                    leftUnion = (SQLUnionQuery) leftLeft;
                    continue;
                } else {
                    rights.add(leftRight);
                    rights.add(leftLeft);
                }
                break;
            }

            for (int i = rights.size() - 1; i >= 0; i--) {
                SQLSelectQuery item = rights.get(i);
                item.accept(this);
            }
            return false;
        }

        return true;
    }

    @Override
    public boolean visit(SQLReplaceStatement x) {
        SQLName tableName = x.getTableName();
        TableMeta tableMeta = getTableStat(tableName);
        accept(x.getColumns());
        accept(x.getValuesList());
        handleInsertValue(tableMeta, x.getColumns(), x.getValuesList());
        accept(x.getQuery());

        return false;
    }

    @Override
    public boolean visit(MySqlExplainStatement x) {
        return visit((SQLExplainStatement) x);
    }

    @Override
    public boolean visit(SQLExplainStatement x) {
        if (x.getStatement() != null) {
            accept(x.getStatement());
        }
        return false;
    }

    @Override
    public boolean visit(SQLBlockStatement x) {
        for (SQLParameter param : x.getParameters()) {
            param.setParent(x);
            param.accept(this);
        }
        for (SQLStatement stmt : x.getStatementList()) {
            stmt.accept(this);
        }
        return false;
    }

    protected void handleValueExprs(SQLExpr expr, List<SQLExpr> values) {
        handleValueExprs(expr, values.toArray(new SQLExpr[values.size()]));
    }

    protected void handleValueExprs(SQLExpr expr, SQLExpr... valueExprs) {
        if (expr instanceof SQLCastExpr) {
            expr = ((SQLCastExpr) expr).getExpr();
        }

        ColumnMeta column = getColumn(expr);

        if (column == null) {
            return;
        }

        for (SQLExpr item : valueExprs) {
            handleVariantRefExpr(item, column);
        }
    }

    private void handleVariantRefExpr(SQLExpr item, ColumnMeta column) {
        if (dynamicParamBindColumn.containsKey(item)) {
            return;
        }
        if (item instanceof SQLVariantRefExpr) {
            SQLVariantRefExpr variantRefExpr = ((SQLVariantRefExpr) item);
            if (variantRefExpr.isGlobal() || variantRefExpr.isSession()) {
                //ignore
            } else if ("?".equalsIgnoreCase(variantRefExpr.getName())) {
                dynamicParamBindColumn.put(variantRefExpr, column);
            } else {
                //ignore
            }
        }
    }

    private void handleSelectItemVariantRefExpr(SQLVariantRefExpr item) {
        handleVariantRefExpr(item, virtualColumn);
    }

    @Override
    public boolean visit(SQLInListExpr x) {
        handleValueExprs(x.getExpr(), x.getTargetList());
        return true;
    }

    @Override
    public boolean visit(SQLDeleteStatement x) {
        if (x.getTableSource() instanceof SQLSubqueryTableSource) {
            SQLSelectQuery selectQuery = ((SQLSubqueryTableSource) x.getTableSource()).getSelect().getQuery();
            if (selectQuery instanceof SQLSelectQueryBlock) {
                SQLSelectQueryBlock subQueryBlock = ((SQLSelectQueryBlock) selectQuery);
                subQueryBlock.getWhere().accept(this);
            }
        }
        getTableStat(x.getTableName());
        final SQLExpr where = x.getWhere();
        if (where != null) {
            where.accept(this);
        }
        return false;
    }

    protected final void statExpr(SQLExpr x) {
        Class<?> clazz = x.getClass();
        if (clazz == SQLBinaryOpExpr.class) {
            visit((SQLBinaryOpExpr) x);
        } else {
            x.accept(this);
        }
    }
}
