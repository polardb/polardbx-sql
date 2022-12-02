package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import org.apache.calcite.rel.ddl.AlterProcedure;
import org.apache.calcite.sql.SqlAlterProcedure;

public class LogicalAlterProcedure extends BaseDdlOperation {
    private SqlAlterProcedure sqlAlterProcedure;

    public LogicalAlterProcedure(AlterProcedure alterProcedure) {
        super(alterProcedure);
        resetSchemaIfNeed(((SqlAlterProcedure) relDdl.sqlNode).getProcedureName());
        this.sqlAlterProcedure = (SqlAlterProcedure) relDdl.sqlNode;
    }

    public static LogicalAlterProcedure create(AlterProcedure alterProcedure) {
        return new LogicalAlterProcedure(alterProcedure);
    }

    public SqlAlterProcedure getSqlAlterProcedure() {
        return sqlAlterProcedure;
    }

    private void resetSchemaIfNeed(SQLName procedureName) {
        if (procedureName instanceof SQLPropertyExpr) {
            setSchemaName(((SQLPropertyExpr) procedureName).getOwnerName());
        }
    }
}
