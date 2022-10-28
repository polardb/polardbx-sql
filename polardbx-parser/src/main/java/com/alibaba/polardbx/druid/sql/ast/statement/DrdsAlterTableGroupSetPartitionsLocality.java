package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * Created by taojinkun.
 *
 * @author taojinkun
 */
public class DrdsAlterTableGroupSetPartitionsLocality extends SQLObjectImpl implements SQLAlterTableGroupItem {

    private SQLExpr targetLocality;

    private SQLName partition;

    private Boolean isLogicalDDL = false;

    public Boolean getLogicalDDL() {
        return isLogicalDDL;
    }

    public void setLogicalDDL(Boolean logicalDDL) {
        isLogicalDDL = logicalDDL;
    }

    public SQLExpr getTargetLocality() {
        return targetLocality;
    }

    public SQLName getPartition() {return partition;}

    public void setPartitionTargetLocality(SQLName partition, SQLExpr targetLocality) {
        this.partition = partition;
        this.targetLocality = targetLocality;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }
}