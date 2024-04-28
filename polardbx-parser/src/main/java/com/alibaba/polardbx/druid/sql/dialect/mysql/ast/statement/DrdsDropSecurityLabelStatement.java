package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLDropStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.List;

/**
 * @author pangzhaoxing
 */
public class DrdsDropSecurityLabelStatement extends MySqlStatementImpl implements SQLDropStatement {

    private List<SQLName> labelNames;

    public DrdsDropSecurityLabelStatement() {
    }

    @Override
    public void accept0(final MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (null != this.labelNames) {
                for (SQLName labelName : labelNames) {
                    labelName.accept(visitor);
                }
            }

        }
        visitor.endVisit(this);
    }

    public List<SQLName> getLabelNames() {
        return labelNames;
    }

    public void setLabelNames(List<SQLName> labelNames) {
        this.labelNames = labelNames;
    }
}
