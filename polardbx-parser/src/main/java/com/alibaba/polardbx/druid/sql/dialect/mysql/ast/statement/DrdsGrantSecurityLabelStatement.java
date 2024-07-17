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

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLGrantStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;
import com.alibaba.polardbx.druid.sql.parser.Token;

/**
 * @author pangzhaoxing
 */
public class DrdsGrantSecurityLabelStatement extends MySqlStatementImpl {

    private SQLExpr policyName;
    private SQLExpr labelName;
    private MySqlUserName userName;
    private SQLExpr accessType;

    @Override
    public void accept0(final MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (null != this.policyName) {
                policyName.accept(visitor);
            }
            if (null != this.labelName) {
                labelName.accept(visitor);
            }
            if (null != this.userName) {
                userName.accept(visitor);
            }
            if (null != this.accessType) {
                accessType.accept(visitor);
            }
        }
        visitor.endVisit(this);
    }

    public SQLExpr getPolicyName() {
        return policyName;
    }

    public void setPolicyName(SQLExpr policyName) {
        this.policyName = policyName;
    }

    public SQLExpr getLabelName() {
        return labelName;
    }

    public void setLabelName(SQLExpr labelName) {
        this.labelName = labelName;
    }

    public MySqlUserName getUserName() {
        return userName;
    }

    public void setUserName(MySqlUserName userName) {
        this.userName = userName;
    }

    public SQLExpr getAccessType() {
        return accessType;
    }

    public void setAccessType(SQLExpr accessType) {
        this.accessType = accessType;
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }
}
