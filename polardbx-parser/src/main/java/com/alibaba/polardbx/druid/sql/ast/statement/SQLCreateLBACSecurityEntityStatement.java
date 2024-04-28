package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * @author pangzhaoxing
 */
public class SQLCreateLBACSecurityEntityStatement extends SQLStatementImpl implements SQLCreateStatement {

    private SQLName entityType;

    private SQLName entityKey;

    private SQLName entityAttr;

    public SQLName getEntityType() {
        return entityType;
    }

    public void setEntityType(SQLName entityType) {
        this.entityType = entityType;
    }

    public SQLName getEntityKey() {
        return entityKey;
    }

    public void setEntityKey(SQLName entityKey) {
        this.entityKey = entityKey;
    }

    public SQLName getEntityAttr() {
        return entityAttr;
    }

    public void setEntityAttr(SQLName entityAttr) {
        this.entityAttr = entityAttr;
    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            if (null != entityType) {
                entityType.accept(v);
            }
            if (null != entityKey) {
                entityKey.accept(v);
            }
            if (null != entityAttr) {
                entityAttr.accept(v);
            }
        }
    }
}
