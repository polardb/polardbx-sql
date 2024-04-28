package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MySqlObjectImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class MySQLInstanceReadonlyItem extends MySqlObjectImpl implements MySqlAlterInstanceItem {

    private List<SQLAssignItem> options = new ArrayList<SQLAssignItem>();

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, options);
        }
        visitor.endVisit(this);
    }

    public List<SQLAssignItem> getOptions() {
        return options;
    }

    public void setOptions(List<SQLAssignItem> options) {
        this.options = options;
    }
}
