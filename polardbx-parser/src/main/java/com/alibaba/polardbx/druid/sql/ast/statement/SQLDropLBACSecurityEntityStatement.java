package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLReplaceable;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.List;

/**
 * @author pangzhaoxing
 */
public class SQLDropLBACSecurityEntityStatement extends SQLStatementImpl implements SQLDropStatement {

    private List<SQLName> entityTypes;

    private List<SQLName> entityKeys;

    public List<SQLName> getEntityTypes() {
        return entityTypes;
    }

    public void setEntityTypes(List<SQLName> entityTypes) {
        this.entityTypes = entityTypes;
    }

    public List<SQLName> getEntityKeys() {
        return entityKeys;
    }

    public void setEntityKeys(List<SQLName> entityKeys) {
        this.entityKeys = entityKeys;
    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            if (null != entityTypes) {
                for (SQLName entityType : entityTypes) {
                    entityType.accept(v);
                }
            }
            if (null != entityKeys) {
                for (SQLName entityKey : entityKeys) {
                    entityKey.accept(v);
                }
            }
        }
    }
}
