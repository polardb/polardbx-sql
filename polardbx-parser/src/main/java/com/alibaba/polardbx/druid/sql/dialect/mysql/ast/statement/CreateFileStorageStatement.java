package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateFileStorageStatement extends MySqlStatementImpl implements SQLCreateStatement {
    private SQLName engineName;
    private List<SQLAssignItem> withValue;

    public CreateFileStorageStatement() {
        this.withValue = new ArrayList<SQLAssignItem>();
    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (this.engineName != null) {
                this.engineName.accept(visitor);
            }
            if (this.withValue != null) {
                for (SQLAssignItem item : withValue) {
                    item.accept(visitor);
                }
            }
        }
        visitor.endVisit(this);
    }

    @Override
    public CreateFileStorageStatement clone() {
        CreateFileStorageStatement x = new CreateFileStorageStatement();
        if (this.engineName != null) {
            x.engineName = this.engineName.clone();
        }
        if (this.withValue != null) {
            for (SQLAssignItem item : withValue) {
                x.withValue.add(item.clone());
            }
        }
        return x;
    }

    public SQLName getEngineName() {
        return engineName;
    }

    public CreateFileStorageStatement setEngineName(SQLName engineName) {
        this.engineName = engineName;
        return this;
    }

    public List<SQLAssignItem> getWithValue() {
        return withValue;
    }
}
