package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class SQLAlterSystemReloadStorageStatement extends SQLStatementImpl implements SQLAlterStatement {

    /**
     * the list of dn id
     */
    private List<SQLExpr> storageList = new ArrayList<>();

    public SQLAlterSystemReloadStorageStatement() {
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, storageList);
        }
        visitor.endVisit(this);
    }

    public List<SQLExpr> getStorageList() {
        return storageList;
    }

    public void setStorageList(List<SQLExpr> storageList) {
        this.storageList = storageList;
    }
}
