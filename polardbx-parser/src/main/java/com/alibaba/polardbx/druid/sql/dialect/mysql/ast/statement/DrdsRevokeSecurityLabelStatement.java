package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.expr.MySqlUserName;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * @author pangzhaoxing
 */
public class DrdsRevokeSecurityLabelStatement extends MySqlStatementImpl {

    private SQLName policyName;
    private MySqlUserName userName;
    private SQLName accessType;

    @Override
    public void accept0(final MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (null != this.policyName) {
                policyName.accept(visitor);
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

    public SQLName getPolicyName() {
        return policyName;
    }

    public void setPolicyName(SQLName policyName) {
        this.policyName = policyName;
    }

    public MySqlUserName getUserName() {
        return userName;
    }

    public void setUserName(MySqlUserName userName) {
        this.userName = userName;
    }

    public SQLName getAccessType() {
        return accessType;
    }

    public void setAccessType(SQLName accessType) {
        this.accessType = accessType;
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }
}
