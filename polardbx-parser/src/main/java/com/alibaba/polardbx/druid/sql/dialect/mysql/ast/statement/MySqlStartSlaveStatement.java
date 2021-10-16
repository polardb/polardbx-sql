
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

import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * @Author ShuGuang
 * @Description
 * @Date 2020/12/25 2:14 下午
 */
public class MySqlStartSlaveStatement extends SQLStatementImpl {
    private SQLCharExpr channel;

    @Override
    protected void accept0(SQLASTVisitor v) {
        v.visit(this);
        v.endVisit(this);
    }

    public SQLCharExpr getChannel() {
        return channel;
    }

    public void setChannel(SQLCharExpr channel) {
        this.channel = channel;
    }
}