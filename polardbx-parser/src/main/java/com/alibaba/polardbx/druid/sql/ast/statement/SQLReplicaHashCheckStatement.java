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

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlStatementImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.List;

/**
 * @author yudong
 * @since 2023/8/23 11:38
 **/
public class SQLReplicaHashCheckStatement extends MySqlStatementImpl {
    private SQLName from;
    private SQLExpr where;
    private List<Object> upperBounds;
    private List<Object> lowerBounds;

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, where);
        }
        visitor.endVisit(this);
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }

    public SQLExpr getWhere() {
        return where;
    }

    public void setWhere(SQLExpr where) {
        this.where = where;
    }

    public SQLName getFrom() {
        return from;
    }

    public void setFrom(SQLName from) {
        this.from = from;
    }

    public void setUpperBounds(List<Object> upperBounds) {
        this.upperBounds = upperBounds;
    }

    public List<Object> getUpperBounds() {
        return upperBounds;
    }

    public void setLowerBounds(List<Object> lowerBounds) {
        this.lowerBounds = lowerBounds;
    }

    public List<Object> getLowerBounds() {
        return lowerBounds;
    }

}
