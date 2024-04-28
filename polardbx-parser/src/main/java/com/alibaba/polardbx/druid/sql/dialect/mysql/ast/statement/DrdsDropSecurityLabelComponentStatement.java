package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.List;

public class DrdsDropSecurityLabelComponentStatement extends MySqlStatementImpl implements SQLDropStatement {

    private List<SQLName> componentNames;

    public DrdsDropSecurityLabelComponentStatement() {
    }

    @Override
    public void accept0(final MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (null != this.componentNames) {
                for (SQLName componentName : componentNames) {
                    componentName.accept(visitor);
                }
            }
        }
        visitor.endVisit(this);
    }

    public List<SQLName> getComponentNames() {
        return componentNames;
    }

    public void setComponentNames(List<SQLName> componentNames) {
        this.componentNames = componentNames;
    }
}
