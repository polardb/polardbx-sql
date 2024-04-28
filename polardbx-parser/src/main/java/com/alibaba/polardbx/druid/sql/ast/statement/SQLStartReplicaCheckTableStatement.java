package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatementImpl;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * @author yudong
 * @since 2023/11/9 11:17
 **/
public class SQLStartReplicaCheckTableStatement extends SQLStatementImpl {

    private SQLName channel;
    private SQLName dbName;
    private SQLName tableName;

    @Override
    protected void accept0(SQLASTVisitor v) {
        v.visit(this);
        v.endVisit(this);
    }

    public SQLName getChannel() {
        return channel;
    }

    public void setChannel(SQLName channel) {
        this.channel = channel;
    }

    public SQLName getDbName() {
        return dbName;
    }

    public void setDbName(SQLName dbName) {
        this.dbName = dbName;
    }

    public SQLName getTableName() {
        return tableName;
    }

    public void setTableName(SQLName tableName) {
        this.tableName = tableName;
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }
}
