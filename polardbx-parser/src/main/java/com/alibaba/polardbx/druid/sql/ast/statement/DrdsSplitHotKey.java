package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class DrdsSplitHotKey extends SQLObjectImpl implements SQLAlterTableItem, SQLAlterTableGroupItem {
    private List<SQLExpr> hotKeys;
    private SQLIntegerExpr partitions;
    private SQLName hotKeyPartitionName;

    public List<SQLExpr> getHotKeys() {
        return hotKeys;
    }

    public void setHotKeys(List<SQLExpr> hotKeys) {
        this.hotKeys = hotKeys;
    }

    public SQLIntegerExpr getPartitions() {
        return partitions;
    }

    public void setPartitions(SQLIntegerExpr partitions) {
        this.partitions = partitions;
    }

    public SQLName getHotKeyPartitionName() {
        return hotKeyPartitionName;
    }

    public void setHotKeyPartitionName(SQLName hotKeyPartitionName) {
        this.hotKeyPartitionName = hotKeyPartitionName;
    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        v.visit(this);
        v.endVisit(this);
    }
}
