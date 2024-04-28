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
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public class SQLImportDatabaseStatement extends SQLStatementImpl {
    private SQLName dstLogicalDb;

    private SQLName srcPhyDb;

    private SQLExpr locality;
    private SQLName status;

    private boolean existStillImportTag = false;

    public SQLName getDstLogicalDb() {
        return dstLogicalDb;
    }

    public void setDstLogicalDb(SQLName dstLogicalDb) {
        this.dstLogicalDb = dstLogicalDb;
    }

    public SQLName getSrcPhyDb() {
        return srcPhyDb;
    }

    public void setSrcPhyDb(SQLName srcPhyDb) {
        this.srcPhyDb = srcPhyDb;
    }

    public SQLExpr getLocality() {
        return locality;
    }

    public void setLocality(SQLExpr locality) {
        this.locality = locality;
    }

    public SQLName getStatus() {
        return status;
    }

    public void setStatus(SQLName status) {
        this.status = status;
    }

    public boolean isExistStillImportTag() {
        return existStillImportTag;
    }

    public void setExistStillImportTag(boolean existStillImportTag) {
        this.existStillImportTag = existStillImportTag;
    }

    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, dstLogicalDb);
            acceptChild(v, srcPhyDb);
            acceptChild(v, locality);
            acceptChild(v, status);
        }
        v.endVisit(this);
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }
}
