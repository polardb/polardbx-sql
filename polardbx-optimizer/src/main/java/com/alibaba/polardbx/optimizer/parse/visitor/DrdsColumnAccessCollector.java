package com.alibaba.polardbx.optimizer.parse.visitor;

import com.alibaba.polardbx.common.privilege.ColumnPrivilegeVerifyItem;
import com.alibaba.polardbx.common.privilege.PrivilegeVerifyItem;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.ast.SQLOrderBy;
import com.alibaba.polardbx.druid.sql.ast.SQLOver;
import com.alibaba.polardbx.druid.sql.ast.SQLWindow;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelect;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectOrderByItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLUpdateSetItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLWithSubqueryClause;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.polardbx.lbac.LBACException;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.planmanager.parametric.Point;
import com.google.common.base.Objects;
import com.taobao.tddl.common.privilege.PrivilegePoint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * collect all accessed columns in SQLStatement
 *
 * @author pangzhaoxing
 */
public class DrdsColumnAccessCollector extends MySqlASTVisitorAdapter {

    private static final String ALL_COLUMN = "*";

    private Context curContext;

    private Context outerContext;

    private Set<ColumnPrivilegeVerifyItem> accessColumnVerifyItems = new HashSet<>();

    private String defaultSchema;

    public DrdsColumnAccessCollector(String defaultSchema) {
        this.defaultSchema = defaultSchema;
        this.curContext = new Context();
        this.outerContext = null;
    }

    public DrdsColumnAccessCollector(String defaultSchema, Context outerContext) {
        this.defaultSchema = defaultSchema;
        this.curContext = new Context();
        this.outerContext = outerContext;
    }

    @Override
    public boolean visit(SQLSelect x) {
        if (x.getWithSubQuery() != null) {
            for (SQLWithSubqueryClause.Entry e : x.getWithSubQuery().getEntries()) {
                e.getSubQuery().accept(this);
            }
        }

        if (x.getQuery() != null) {
            x.getQuery().accept(this);
        }
        return false;
    }

    @Override
    public boolean visit(MySqlSelectQueryBlock x) {
        return visit((SQLSelectQueryBlock) x);
    }

    @Override
    public boolean visit(SQLSelectQueryBlock x) {
        //用于解决subquery问题，这样在其他任意使用subquery的地方都不需要特殊处理
        DrdsColumnAccessCollector collector = new DrdsColumnAccessCollector(defaultSchema, curContext);
        collector.visitQuery(x);
        this.accessColumnVerifyItems.addAll(collector.getAccessColumnVerifyItems());
        return false;
    }

    private void visitQuery(SQLSelectQueryBlock x) {
        if (x.getFrom() != null) {
            x.getFrom().accept(this);
        }
        if (x.getSelectList() != null) {
            for (SQLSelectItem item : x.getSelectList()) {
                item.accept(this);
            }
        }
        if (x.getWhere() != null) {
            visitExpr(x.getWhere(), PrivilegePoint.SELECT);
        }
        if (x.getGroupBy() != null) {
            x.getGroupBy().accept(this);
        }
        if (x.getOrderBy() != null) {
            x.getOrderBy().accept(this);
        }
        List<SQLWindow> windows = x.getWindows();
        if (windows != null) {
            for (SQLWindow window : windows) {
                window.accept(this);
            }
        }
    }

    @Override
    public boolean visit(SQLSelectItem item) {
        SQLExpr sqlExpr = item.getExpr();
        visitExpr(sqlExpr, PrivilegePoint.SELECT);
        return false;
    }

    @Override
    public boolean visit(SQLSelectGroupByClause groupBy) {
        for (SQLExpr expr : groupBy.getItems()) {
            visitExpr(expr, PrivilegePoint.SELECT);
        }
        if (groupBy.getHaving() != null) {
            visitExpr(groupBy.getHaving(), PrivilegePoint.SELECT);
        }
        return false;
    }

    @Override
    public boolean visit(SQLOrderBy orderBy) {
        for (SQLSelectOrderByItem item : orderBy.getItems()) {
            visitExpr(item.getExpr(), PrivilegePoint.SELECT);
        }
        return false;
    }

    @Override
    public boolean visit(SQLWindow window) {
        window.getOver().accept(this);
        return false;
    }

    @Override
    public boolean visit(SQLOver over) {
        if (over.getPartitionBy() != null) {
            for (SQLExpr expr : over.getPartitionBy()) {
                visitExpr(expr, PrivilegePoint.SELECT);
            }
        }
        if (over.getOrderBy() != null) {
            over.getOrderBy().accept(this);
        }
        if (over.getDistributeBy() != null) {
            over.getDistributeBy().accept(this);
        }
        if (over.getSortBy() != null) {
            over.getSortBy().accept(this);
        }
        return false;
    }

    //收集表达式中的列
    // 列的格式：column/table.column/schema.table.column
    public boolean visitExpr(SQLExpr sqlExpr, PrivilegePoint... points) {
        if (sqlExpr == null) {
            return false;
        }
        if (sqlExpr instanceof SQLIdentifierExpr) {
            accessColumn(null, null, ((SQLIdentifierExpr) sqlExpr).getName(), points);
        } else if (sqlExpr instanceof SQLPropertyExpr) {
            SQLPropertyExpr propertyExpr = (SQLPropertyExpr) sqlExpr;
            String col = propertyExpr.getName();
            SQLExpr owner = propertyExpr.getOwner();
            if (owner instanceof SQLPropertyExpr) {
                String db = ((SQLPropertyExpr) owner).getOwnerName();
                String tb = ((SQLPropertyExpr) owner).getName();
                accessColumn(db, tb, col, points);
            } else {
                String tb = ((SQLIdentifierExpr) owner).getName();
                accessColumn(null, tb, col, points);
            }
        } else if (sqlExpr instanceof SQLAllColumnExpr) {
            accessColumn(null, null, ALL_COLUMN, points);
        } else {
            for (SQLObject child : sqlExpr.getChildren()) {
                if (child instanceof SQLExpr) {
                    visitExpr((SQLExpr) child, points);
                } else {
                    child.accept(this);
                }
            }
        }
        return false;
    }

    @Override
    public boolean visit(SQLExprTableSource from) {
        String db = defaultSchema;
        String tb = null;
        SQLExpr expr = from.getExpr();
        if (expr instanceof SQLPropertyExpr) {
            db = ((SQLPropertyExpr) expr).getOwnerName();
            tb = ((SQLPropertyExpr) expr).getName();
        } else if (expr instanceof SQLIdentifierExpr) {
            tb = ((SQLIdentifierExpr) expr).getName();
        }
        curContext.addTable(db, tb);
        if (from.getAlias() != null) {
            curContext.addTableAlias(db, tb, from.getAlias());
        }
        return false;
    }

    @Override
    public boolean visit(SQLSubqueryTableSource from) {
        from.getSelect().accept(this);
        return false;
    }

    @Override
    public boolean visit(SQLJoinTableSource from) {
        SQLTableSource left = from.getLeft();
        SQLTableSource right = from.getRight();
        left.accept(this);
        right.accept(this);
        SQLExpr condition = from.getCondition();
        visitExpr(condition, PrivilegePoint.SELECT);
        return false;
    }

    @Override
    public boolean visit(MySqlUpdateStatement x) {
        if (x.getTableSource() != null) {
            x.getTableSource().accept(this);
        }
        if (x.getItems() != null) {
            for (SQLUpdateSetItem item : x.getItems()) {
                item.accept(this);
            }
        }
        visitExpr(x.getWhere(), PrivilegePoint.SELECT);
        if (x.getOrderBy() != null) {
            x.getOrderBy().accept(this);
        }
        return false;
    }

    @Override
    public boolean visit(SQLUpdateSetItem item) {
        visitExpr(item.getColumn(), PrivilegePoint.UPDATE);
        visitExpr(item.getValue(), PrivilegePoint.SELECT);
        return false;
    }

    @Override
    public boolean visit(MySqlInsertStatement x) {
        if (x.getTableSource() != null) {
            x.getTableSource().accept(this);
        }
        if (x.getColumns() == null || x.getColumns().isEmpty()) {
            accessColumn(null, null, ALL_COLUMN, PrivilegePoint.INSERT);
        } else {
            for (SQLExpr expr : x.getColumns()) {
                visitExpr(expr, PrivilegePoint.INSERT);
            }
        }
        if (x.getQuery() != null) {
            x.getQuery().accept(this);
        }
        return false;
    }

    @Override
    public boolean visit(MySqlDeleteStatement x) {
        if (x.getTableSource() != null) {
            x.getTableSource().accept(this);
        }
        accessColumn(null, null, ALL_COLUMN, PrivilegePoint.DELETE);
        visitExpr(x.getWhere(), PrivilegePoint.SELECT);
        if (x.getOrderBy() != null) {
            x.getOrderBy().accept(this);
        }
        return false;
    }

    @Override
    public boolean visit(SQLReplaceStatement x) {
        if (x.getTableSource() != null) {
            x.getTableSource().accept(this);
        }
        if (x.getColumns() == null || x.getColumns().isEmpty()) {
            accessColumn(null, null, ALL_COLUMN, PrivilegePoint.INSERT, PrivilegePoint.UPDATE);
        } else {
            for (SQLExpr expr : x.getColumns()) {
                visitExpr(expr, PrivilegePoint.INSERT, PrivilegePoint.UPDATE);
            }
        }
        if (x.getQuery() != null) {
            x.getQuery().accept(this);
        }
        return false;
    }

    public Set<ColumnPrivilegeVerifyItem> getAccessColumnVerifyItems() {
        return accessColumnVerifyItems;
    }

    private void accessColumn(String db, String tb, String col, PrivilegePoint... points) {
        db = SQLUtils.normalizeNoTrim(db);
        tb = SQLUtils.normalizeNoTrim(tb);
        col = SQLUtils.normalizeNoTrim(col);

        if (col == null) {
            return;
        }

        if (ALL_COLUMN.equals(col)) {
            for (String[] fullColName : curContext.buildAllColumn()) {
                addPrivilegeVerifyItems(fullColName[0], fullColName[1], fullColName[2], points);
            }
            return;
        }

        if (tb == null) {
            Pair<String, String> fullTbName = curContext.findTable(col);
            if (fullTbName == null && outerContext != null) {
                fullTbName = outerContext.findTable(col);

            }
            if (fullTbName == null) {
                throw new LBACException("unknown column : " + col);
            }
            addPrivilegeVerifyItems(fullTbName.getKey(), fullTbName.getValue(), col, points);
            return;
        }

        if (db == null) {
            Pair<String, String> fullTbName = curContext.findTable(tb, col);
            if (fullTbName == null && outerContext != null) {
                fullTbName = outerContext.findTable(tb, col);
            }
            if (fullTbName == null) {
                throw new LBACException("unknown column : " + col);
            }
            addPrivilegeVerifyItems(fullTbName.getKey(), fullTbName.getValue(), col, points);
            return;
        }

        addPrivilegeVerifyItems(db, tb, col, points);
    }

    private void addPrivilegeVerifyItems(String db, String tb, String col, PrivilegePoint... points) {
        for (PrivilegePoint point : points) {
            accessColumnVerifyItems.add(new ColumnPrivilegeVerifyItem(db, tb, col, point));
        }
    }

    private class Context {
        // key : schemaName value : tableNames
        private Map<String, Set<String>> tableNames = new HashMap<>();

        //key : alias  value: tableName
        private Map<String, Pair<String, String>> tableAlias = new HashMap<>();

        public void addTable(String db, String table) {
            tableNames.putIfAbsent(db, new HashSet<>());
            tableNames.get(db).add(table);
        }

        public void addTableAlias(String db, String tb, String alias) {
            tableAlias.put(alias, Pair.of(db, tb));
        }

        public Pair<String, String> findTable(String col) {
            for (Map.Entry<String, Set<String>> entry : tableNames.entrySet()) {
                OptimizerContext optimizerContext = OptimizerContext.getContext(entry.getKey());
                if (optimizerContext == null) {
                    continue;
                }
                SchemaManager manager = optimizerContext.getLatestSchemaManager();
                for (String tableName : entry.getValue()) {
                    TableMeta meta = manager.getTable(tableName);
                    if (meta.getColumn(col) != null) {
                        return Pair.of(defaultSchema, meta.getTableName());
                    }
                }
            }
            return null;
        }

        public Pair<String, String> findTable(String tb, String col) {
            if (tableAlias.containsKey(tb)) {
                return tableAlias.get(tb);
            }

            Set<String> curTables = tableNames.get(defaultSchema);
            if (curTables != null && curTables.contains(tb)) {
                return new Pair<>(defaultSchema, tb);
            }

            return null;
        }

        public List<String[]> buildAllColumn() {
            List<String[]> allColumns = new ArrayList<>();
            for (Map.Entry<String, Set<String>> entry : tableNames.entrySet()) {
                String db = entry.getKey();
                for (String tb : entry.getValue()) {
                    OptimizerContext optimizerContext = OptimizerContext.getContext(db);
                    if (optimizerContext == null) {
                        continue;
                    }
                    SchemaManager manager = optimizerContext.getLatestSchemaManager();
                    TableMeta tbMeta = manager.getTable(tb);
                    for (ColumnMeta colMeta : tbMeta.getAllColumns()) {
                        allColumns.add(new String[] {db, tb, colMeta.getName()});
                    }
                }
            }
            return allColumns;
        }

    }

}
