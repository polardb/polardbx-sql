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
package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlObjectImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

public class DrdsAlterTablePartition extends MySqlObjectImpl implements SQLAlterTableItem {

    protected SQLExpr           dbPartitionBy;//for drds
    protected SQLExpr           dbPartitions;//for drds
    protected SQLExpr           tablePartitionBy;//for drds
    protected SQLExpr           tablePartitions;//for drds
    protected MySqlExtPartition exPartition; //for drds

    public DrdsAlterTablePartition(){

    }

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, dbPartitionBy);
            acceptChild(visitor, dbPartitions);
            acceptChild(visitor, tablePartitionBy);
            acceptChild(visitor, tablePartitions);
            acceptChild(visitor, exPartition);

        }
        visitor.endVisit(this);
    }

    public SQLExpr getDbPartitionBy() {
        return dbPartitionBy;
    }

    public void setDbPartitionBy(SQLExpr dbPartitionBy) {
        this.dbPartitionBy = dbPartitionBy;
    }

    public SQLExpr getDbPartitions() {
        return dbPartitions;
    }

    public void setDbPartitions(SQLExpr dbPartitions) {
        this.dbPartitions = dbPartitions;
    }

    public SQLExpr getTablePartitionBy() {
        return tablePartitionBy;
    }

    public void setTablePartitionBy(SQLExpr tablePartitionBy) {
        this.tablePartitionBy = tablePartitionBy;
    }

    public SQLExpr getTablePartitions() {
        return tablePartitions;
    }

    public void setTablePartitions(SQLExpr tablePartitions) {
        this.tablePartitions = tablePartitions;
    }
}
