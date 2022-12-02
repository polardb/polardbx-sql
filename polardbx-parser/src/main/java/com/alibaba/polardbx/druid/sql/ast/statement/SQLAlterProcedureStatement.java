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
package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public class SQLAlterProcedureStatement extends SQLStatementImpl implements SQLAlterStatement {

    private SQLName name;

    private boolean compile = false;
    private boolean reuseSettings = false;

    private SQLCharExpr comment;
    private boolean existsComment;

    private boolean languageSql;
    private boolean existsLanguageSql;

    private SqlDataAccess sqlDataAccess;
    private boolean existsSqlDataAccess;

    private SqlSecurity sqlSecurity = SqlSecurity.DEFINER;
    private boolean existsSqlSecurity;

    @Override
    public void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, name);
        }
        visitor.endVisit(this);
    }

    public SQLName getName() {
        return name;
    }

    public void setName(SQLName name) {
        this.name = name;
    }

    public boolean isCompile() {
        return compile;
    }

    public void setCompile(boolean compile) {
        this.compile = compile;
    }

    public boolean isReuseSettings() {
        return reuseSettings;
    }

    public void setReuseSettings(boolean reuseSettings) {
        this.reuseSettings = reuseSettings;
    }

    public boolean isLanguageSql() {
        return languageSql;
    }

    public void setLanguageSql(boolean languageSql) {
        this.languageSql = languageSql;
        this.existsLanguageSql = true;
    }

    public SqlSecurity getSqlSecurity() {
        return sqlSecurity;
    }

    public void setSqlSecurity(SqlSecurity sqlSecurity) {
        this.sqlSecurity = sqlSecurity;
        this.existsSqlSecurity = true;
    }

    public SQLCharExpr getComment() {
        return comment;
    }

    public void setComment(SQLCharExpr comment) {
        if (comment != null) {
            comment.setParent(this);
        }
        this.comment = comment;
        this.existsComment = true;
    }

    public boolean isExistsComment() {
        return existsComment;
    }

    public boolean isExistsLanguageSql() {
        return existsLanguageSql;
    }

    public boolean isExistsSqlSecurity() {
        return existsSqlSecurity;
    }

    public boolean isExistsSqlDataAccess() {
        return existsSqlDataAccess;
    }

    public SqlDataAccess getSqlDataAccess() {
        return sqlDataAccess;
    }

    public void setSqlDataAccess(SqlDataAccess sqlDataAccess) {
        this.sqlDataAccess = sqlDataAccess;
        this.existsSqlDataAccess = true;
    }
}
