package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author pangzhaoxing
 */
public class DrdsCreateSecurityLabelStatement extends MySqlStatementImpl implements SQLCreateStatement {

    private SQLExpr labelName;
    private SQLExpr policyName;
    private SQLExpr labelContent;

    @Override
    public void accept0(final MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (null != this.labelName) {
                labelName.accept(visitor);
            }
            if (null != this.policyName) {
                policyName.accept(visitor);
            }
            if (null != this.labelContent) {
                labelContent.accept(visitor);
            }
        }
        visitor.endVisit(this);
    }

    public SQLExpr getLabelName() {
        return labelName;
    }

    public void setLabelName(SQLExpr labelName) {
        this.labelName = labelName;
    }

    public SQLExpr getPolicyName() {
        return policyName;
    }

    public void setPolicyName(SQLExpr policyName) {
        this.policyName = policyName;
    }

    public SQLExpr getLabelContent() {
        return labelContent;
    }

    public void setLabelContent(SQLExpr labelContent) {
        this.labelContent = labelContent;
    }
}
