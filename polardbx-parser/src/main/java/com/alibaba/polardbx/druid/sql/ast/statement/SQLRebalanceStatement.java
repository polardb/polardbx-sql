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

package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class SQLRebalanceStatement extends SQLStatementImpl {


    public enum RebalanceTarget {
        TABLE,
        TABLEGROUP,
        DATABASE,
        CLUSTER
    }

    private RebalanceTarget target;
    private SQLExprTableSource tableSource;
    private List<SQLAssignItem> options = new ArrayList<SQLAssignItem>();
    private boolean logicalDdl = false;

    public SQLRebalanceStatement() {
        super (DbType.mysql);
    }

    public void setRebalanceTable() {
        this.target = RebalanceTarget.TABLE;
    }

    public void setRebalanceTableGroup(){
        this.target = RebalanceTarget.TABLEGROUP;
    }
    
    public void setRebalanceDatabase() {
        this.target = RebalanceTarget.DATABASE;
    }

    public void setRebalanceCluster() {
        this.target = RebalanceTarget.CLUSTER;
    }

    public RebalanceTarget getRebalanceTarget() {
        return this.target;
    }

    public boolean isRebalanceTable() {
        return this.target.equals(RebalanceTarget.TABLE);
    }

    public boolean isRebalanceCluster() {
        return this.target.equals(RebalanceTarget.CLUSTER);
    }

    public boolean isRebalanceDatabase() {
        return this.target.equals(RebalanceTarget.DATABASE);
    }

    public SQLExprTableSource getTableSource() {
        return tableSource;
    }

    public void setTableSource(SQLExpr table) {
        if (table == null) {
            return;
        }

        setTableSource(new SQLExprTableSource(table));
    }

    public void setTableSource(SQLExprTableSource tableSource) {
        this.tableSource = tableSource;
    }

    public List<SQLAssignItem> getOptions() {
        return this.options;
    }

    public void addOption(SQLAssignItem option) {
        this.options.add(option);
    }

    public void addOption(String name, SQLExpr value) {
        SQLAssignItem assignItem = new SQLAssignItem(new SQLIdentifierExpr(name), value);
        assignItem.setParent(this);
        this.addOption(assignItem);
    }

    public void setOptions(List<SQLAssignItem> options) {
        this.options = options;
    }

    public boolean isLogicalDdl() {
        return logicalDdl;
    }

    public void setLogicalDdl(boolean logicalDdl) {
        this.logicalDdl = logicalDdl;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, tableSource);
            acceptChild(visitor, options);
        }
        visitor.endVisit(this);
    }
}

