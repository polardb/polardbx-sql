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

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * @author yudong
 * @since 2023/11/9 11:18
 **/
public class SQLCancelReplicaCheckTableStatement extends SQLStatementImpl {

    private SQLName dbName = null;
    private SQLName tableName = null;

    public void accept0(SQLASTVisitor v) {
        v.visit(this);
        v.endVisit(this);
    }

    public void setDbName(SQLName dbName) {
        this.dbName = dbName;
    }

    public SQLName getDbName() {
        return dbName;
    }

    public void setTableName(SQLName tableName) {
        this.tableName = tableName;
    }

    public SQLName getTableName() {
        return tableName;
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }
}
