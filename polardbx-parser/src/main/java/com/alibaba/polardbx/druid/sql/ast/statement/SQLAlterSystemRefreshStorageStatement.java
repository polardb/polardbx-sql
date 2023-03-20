package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class SQLAlterSystemRefreshStorageStatement extends SQLStatementImpl implements SQLAlterStatement {

    private SQLExpr targetStorage;
    private List<SQLAssignItem> assignItems = new ArrayList<SQLAssignItem>();

    public SQLAlterSystemRefreshStorageStatement() {
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, targetStorage);
            acceptChild(visitor, assignItems);
        }
        visitor.endVisit(this);
    }

    public SQLExpr getTargetStorage() {
        return targetStorage;
    }

    public void setTargetStorage(SQLExpr targetStorage) {
        this.targetStorage = targetStorage;
    }

    public List<SQLAssignItem> getAssignItems() {
        return assignItems;
    }

    public void setAssignItems(List<SQLAssignItem> assignItems) {
        this.assignItems = assignItems;
    }
}
