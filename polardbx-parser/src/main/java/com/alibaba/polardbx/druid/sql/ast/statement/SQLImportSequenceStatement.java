package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class SQLImportSequenceStatement extends SQLStatementImpl {

    private SQLName logicalDatabase;

    public SQLName getLogicalDatabase() {
        return logicalDatabase;
    }

    public void setLogicalDatabase(SQLName logicalDatabase) {
        this.logicalDatabase = logicalDatabase;
    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, logicalDatabase);
        }
        v.endVisit(this);
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }

}
