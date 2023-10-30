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

package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * @author Shi Yuxuan
 */
public class DrdsUnArchiveStatement extends MySqlStatementImpl implements SQLStatement {
    public enum UnArchiveTarget {
        TABLE,
        TABLE_GROUP,
        DATABASE
    }

    private UnArchiveTarget target;
    private SQLExprTableSource tableSource;
    private SQLName tableGroup;
    private SQLName database;

    public void accept0(MySqlASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public void setTable(SQLExprTableSource table) {
        this.target = UnArchiveTarget.TABLE;
        this.tableSource = table;
    }

    public void setDatabase(SQLName database) {
        this.target = UnArchiveTarget.DATABASE;
        this.database = database;
    }

    public void setTableGroup(SQLName tableGroup) {
        this.target = UnArchiveTarget.TABLE_GROUP;
        this.tableGroup = tableGroup;
    }

    public UnArchiveTarget getTarget() {
        return target;
    }

    public SQLExprTableSource getTableSource() {
        return tableSource;
    }

    public SQLName getTableGroup() {
        return tableGroup;
    }

    public SQLName getDatabase() {
        return database;
    }

    @Override
    public String toString() {
        if (target == null) {
            return "";
        }
        return super.toString();
    }

    @Override
    public SqlType getSqlType() {
        return SqlType.UNARCHIVE;
    }
}
