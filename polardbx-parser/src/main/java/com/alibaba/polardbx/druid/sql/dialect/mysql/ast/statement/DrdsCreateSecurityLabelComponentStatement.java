package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * @author pangzhaoxing
 */
public class DrdsCreateSecurityLabelComponentStatement extends MySqlStatementImpl implements SQLCreateStatement {

    private SQLExpr componentName;
    private SQLExpr componentType;
    private SQLExpr componentContent;

    @Override
    public void accept0(final MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (null != this.componentName) {
                componentName.accept(visitor);
            }
            if (null != this.componentType) {
                componentType.accept(visitor);
            }
            if (null != this.componentContent) {
                componentContent.accept(visitor);
            }
        }
        visitor.endVisit(this);
    }

    public SQLExpr getComponentName() {
        return componentName;
    }

    public void setComponentName(SQLExpr componentName) {
        this.componentName = componentName;
    }

    public SQLExpr getComponentType() {
        return componentType;
    }

    public void setComponentType(SQLExpr componentType) {
        this.componentType = componentType;
    }

    public SQLExpr getComponentContent() {
        return componentContent;
    }

    public void setComponentContent(SQLExpr componentContent) {
        this.componentContent = componentContent;
    }
}
