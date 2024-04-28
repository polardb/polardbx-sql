package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * @author pangzhaoxing
 */
public class DrdsCreateSecurityPolicyStatement extends MySqlStatementImpl implements SQLCreateStatement {

    private SQLExpr policyName;
    private SQLExpr policyComponents;

    @Override
    public void accept0(final MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (null != this.policyName) {
                policyName.accept(visitor);
            }
            if (null != this.policyComponents) {
                policyComponents.accept(visitor);
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

    public SQLExpr getPolicyComponents() {
        return policyComponents;
    }

    public void setPolicyComponents(SQLExpr policyComponents) {
        this.policyComponents = policyComponents;
    }
}
