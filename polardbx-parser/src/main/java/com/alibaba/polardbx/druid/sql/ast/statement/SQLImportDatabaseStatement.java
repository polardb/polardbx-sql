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
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public class SQLImportDatabaseStatement extends SQLStatementImpl {
    private SQLName db;
    private SQLName status;

    public SQLName getDb() {
        return db;
    }

    public void setDb(SQLName db) {
        this.db = db;
    }

    public SQLName getStatus() {
        return status;
    }

    public void setStatus(SQLName status) {
        this.status = status;
    }

    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, db);
            acceptChild(v, status);
        }
        v.endVisit(this);
    }
}
