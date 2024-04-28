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
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class SQLImportSequenceStatement extends SQLStatementImpl {

    private SQLName logicalDatabase;

    public SQLName getLogicalDatabase() {
        return logicalDatabase;
    }

    public void setLogicalDatabase(SQLName logicalDatabase) {
        this.logicalDatabase = logicalDatabase;
    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, logicalDatabase);
        }
        v.endVisit(this);
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }

}
