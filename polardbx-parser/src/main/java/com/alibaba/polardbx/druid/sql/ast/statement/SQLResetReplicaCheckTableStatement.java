package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * @author yudong
 * @since 2023/11/9 11:19
 **/
public class SQLResetReplicaCheckTableStatement extends SQLStatementImpl {

    private SQLName dbName = null;
    private SQLName tableName = null;

    public void accept0(SQLASTVisitor v) {
        v.visit(this);
        v.endVisit(this);
    }

    public void setDbName(SQLName dbName) {
        this.dbName = dbName;
    }

    public SQLName getDbName() {
        return dbName;
    }

    public void setTableName(SQLName tableName) {
        this.tableName = tableName;
    }

    public SQLName getTableName() {
        return tableName;
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }
}
