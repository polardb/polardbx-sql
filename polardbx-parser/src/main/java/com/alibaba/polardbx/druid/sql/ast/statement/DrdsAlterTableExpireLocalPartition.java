package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class DrdsAlterTableExpireLocalPartition extends SQLObjectImpl implements SQLAlterTableItem {

    private List<SQLName> partitions  = new ArrayList<SQLName>(4);

    public void setPartitions(final List<SQLName> partitions) {
        this.partitions = partitions;
    }

    public List<SQLName> getPartitions() {
        return this.partitions;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }
}