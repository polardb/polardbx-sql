package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;
import com.alibaba.polardbx.druid.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by taojinkun.
 *
 * @author taojinkun
 */
public class DrdsAlterTableGroupSetLocality extends SQLObjectImpl implements SQLAlterTableGroupItem {

    private SQLExpr targetLocality;

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


    public void setTargetLocality(SQLExpr targetLocality) {
        this.targetLocality = targetLocality;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }
}