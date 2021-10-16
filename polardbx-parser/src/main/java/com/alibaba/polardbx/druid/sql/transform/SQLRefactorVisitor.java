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

package com.alibaba.polardbx.druid.sql.transform;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLObject;
import com.alibaba.polardbx.druid.sql.ast.SQLOrderBy;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLAggregateExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectGroupByClause;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.polardbx.druid.sql.repository.SchemaObject;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitorAdapter;
import com.alibaba.polardbx.druid.util.FnvHash;

import java.util.HashMap;
import java.util.Map;

public class SQLRefactorVisitor extends SQLASTVisitorAdapter {
    private int havingLevel = 0;
    private int groupByLevel = 0;

    private char quote = '"';
    public SQLRefactorVisitor(DbType dbType) {
        this.dbType = dbType;

        switch (dbType) {
            case mysql:
                quote = '`';
                break;
            default:
                break;
        }
    }

    private Map<Long, TableMapping> tableMappings = new HashMap<Long, TableMapping>();

    public void addMapping(TableMapping mapping) {
        tableMappings.put(mapping.getSrcTableHash(), mapping);
    }

    public boolean visit(SQLExprTableSource x) {
        TableMapping mapping =findMapping(x);
        if (mapping == null) {
            return true;
        }

        String destTable = mapping.getDestTable();

        x.setExpr(new SQLIdentifierExpr(quote(destTable)));

        return false;
    }

    private TableMapping findMapping(SQLExprTableSource x) {
        SchemaObject schemaObject = x.getSchemaObject();
        if (schemaObject == null) {
            return null;
        }

        long nameHashCode = FnvHash.hashCode64(schemaObject.getName());
        return tableMappings.get(nameHashCode);
    }

    public boolean visit(SQLIdentifierExpr x) {
        TableMapping mapping = null;

        if (groupByLevel > 0 || havingLevel > 0) {
            SQLSelectQueryBlock queryBlock = null;
            for (SQLObject parent = x.getParent();
                 parent != null;
                 parent = parent.getParent()
            ) {
                if (parent instanceof SQLSelectQueryBlock) {
                    queryBlock = (SQLSelectQueryBlock) parent;
                    break;
                }
            }

            boolean matchAlias = false;
            if (queryBlock != null) {
                for (SQLSelectItem item : queryBlock.getSelectList()) {
                    if (item.alias_hash() == x.hashCode64()) {
                        matchAlias = true;
                        break;
                    }
                }
            }

            if (matchAlias) {
                SQLObject parent = x.getParent();
                if (parent instanceof SQLOrderBy
                        || parent instanceof SQLSelectGroupByClause) {
                    return false;
                }

                if (havingLevel > 0) {
                    boolean agg = false;
                    for (; parent != null; parent = parent.getParent()) {
                        if (parent instanceof SQLSelectQueryBlock) {
                            break;
                        }

                        if (parent instanceof SQLAggregateExpr) {
                            agg = true;
                            break;
                        }
                    }
                    if (!agg) {
                        return false;
                    }
                }

            }
        }

        SQLObject ownerObject = x.getResolvedOwnerObject();
        if (ownerObject instanceof SQLExprTableSource) {
            mapping = findMapping((SQLExprTableSource) ownerObject);
        }

        if (mapping == null) {
            return false;
        }

        String srcName = x.getName();
        String mappingColumn = mapping.getMappingColumn(srcName);
        if (mappingColumn != null) {
            x.setName(quote(mappingColumn));
        }

        SQLObject parent = x.getParent();
        if (parent instanceof SQLSelectItem
                && ((SQLSelectItem) parent).getAlias() == null) {
            ((SQLSelectItem) parent).setAlias(srcName);
        }

        return false;
    }

    public boolean visit(SQLSelectGroupByClause x) {
        groupByLevel++;
        for (SQLExpr item : x.getItems()) {
            item.accept(this);
        }

        SQLExpr having = x.getHaving();
        if (having != null) {
            havingLevel++;
            having.accept(this);
            havingLevel--;
        }
        groupByLevel--;

        return false;
    }

    public boolean visit(SQLPropertyExpr x) {
        TableMapping mapping = null;
        SchemaObject schemaObject = null;

        boolean aliasOwer = false;
        SQLObject ownerObject = x.getResolvedOwnerObject();
        if (ownerObject instanceof SQLExprTableSource) {
            SQLExprTableSource exprTableSource = (SQLExprTableSource) ownerObject;
            if (exprTableSource.getAlias() != null && x.getOwner() instanceof SQLIdentifierExpr) {
                if (FnvHash.hashCode64(exprTableSource.getAlias()) == ((SQLIdentifierExpr) x.getOwner()).nameHashCode64()) {
                    aliasOwer = true;
                }
            }

            mapping = findMapping(exprTableSource);
            schemaObject = (exprTableSource).getSchemaObject();
        }

        if (mapping == null) {
            return false;
        }

        String srcName = x.getName();
        String mappingColumn = mapping.getMappingColumn(srcName);
        if (mappingColumn != null) {
            x.setName(quote(mappingColumn));
        }

        SQLObject parent = x.getParent();
        if (parent instanceof SQLSelectItem
                && ((SQLSelectItem) parent).getAlias() == null) {
            ((SQLSelectItem) parent).setAlias(srcName);
        }

        if (x.getOwner() instanceof SQLIdentifierExpr
                && ((SQLIdentifierExpr) x.getOwner()).nameHashCode64() == mapping.getSrcTableHash()
                && !aliasOwer
        ) {
            x.setOwner(new SQLIdentifierExpr(quote(mapping.getDestTable())));
        }

        return false;
    }


    private String quote(String name) {
        char[] chars = new char[name.length() + 2];
        name.getChars(0, name.length(), chars, 1);
        chars[0] = '`';
        chars[chars.length - 1] = '`';
        return new String(chars);
    }
}
