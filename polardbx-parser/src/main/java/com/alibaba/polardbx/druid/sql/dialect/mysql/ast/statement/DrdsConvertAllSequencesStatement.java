package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

public class DrdsConvertAllSequencesStatement extends MySqlStatementImpl implements SQLStatement {

    private SQLName fromType;
    private SQLName toType;
    private SQLName schemaName;
    private boolean allSchemata;

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public SQLName getFromType() {
        return fromType;
    }

    public void setFromType(SQLName fromType) {
        this.fromType = fromType;
    }

    public SQLName getToType() {
        return toType;
    }

    public void setToType(SQLName toType) {
        this.toType = toType;
    }

    public SQLName getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(SQLName schemaName) {
        this.schemaName = schemaName;
    }

    public boolean isAllSchemata() {
        return allSchemata;
    }

    public void setAllSchemata(boolean allSchemata) {
        this.allSchemata = allSchemata;
    }
}
