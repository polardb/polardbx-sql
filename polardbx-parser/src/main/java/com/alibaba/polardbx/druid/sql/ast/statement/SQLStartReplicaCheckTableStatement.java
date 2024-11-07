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
 * @since 2023/11/9 11:17
 **/
public class SQLStartReplicaCheckTableStatement extends SQLStatementImpl {

    private SQLName channel;
    private SQLName dbName;
    private SQLName tableName;
    private SQLName mode;

    @Override
    protected void accept0(SQLASTVisitor v) {
        v.visit(this);
        v.endVisit(this);
    }

    public SQLName getChannel() {
        return channel;
    }

    public void setChannel(SQLName channel) {
        this.channel = channel;
    }

    public SQLName getDbName() {
        return dbName;
    }

    public void setDbName(SQLName dbName) {
        this.dbName = dbName;
    }

    public SQLName getTableName() {
        return tableName;
    }

    public void setTableName(SQLName tableName) {
        this.tableName = tableName;
    }

    public SQLName getMode() {
        return mode;
    }

    public void setMode(SQLName mode) {
        this.mode = mode;
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }
}
