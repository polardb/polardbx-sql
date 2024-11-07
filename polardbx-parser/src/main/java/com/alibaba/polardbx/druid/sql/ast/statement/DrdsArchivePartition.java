package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.List;

/**
 * @author wumu
 */
public class DrdsArchivePartition extends SQLObjectImpl implements SQLAlterTableItem {
    private List<SQLName> partitions;

    private boolean subPartitionsArchive;

    public List<SQLName> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<SQLName> partitions) {
        this.partitions = partitions;
    }

    public boolean isSubPartitionsArchive() {
        return subPartitionsArchive;
    }

    public void setSubPartitionsArchive(boolean subPartitionsArchive) {
        this.subPartitionsArchive = subPartitionsArchive;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }
}