package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.List;

/**
 * @author pangzhaoxing
 */
public class DrdsDropSecurityPolicyStatement extends MySqlStatementImpl implements SQLDropStatement {

    private List<SQLName> policyNames;

    public DrdsDropSecurityPolicyStatement() {
    }

    @Override
    public void accept0(final MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (null != this.policyNames) {
                for (SQLName policyName : policyNames) {
                    policyName.accept(visitor);
                }
            }
        }
        visitor.endVisit(this);
    }

    public List<SQLName> getPolicyNames() {
        return policyNames;
    }

    public void setPolicyNames(List<SQLName> policyNames) {
        this.policyNames = policyNames;
    }
}
