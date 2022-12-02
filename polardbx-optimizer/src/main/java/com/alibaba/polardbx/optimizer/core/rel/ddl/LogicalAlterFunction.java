package com.alibaba.polardbx.optimizer.core.rel.ddl;

import org.apache.calcite.rel.ddl.AlterFunction;
import org.apache.calcite.sql.SqlAlterFunction;

public class LogicalAlterFunction extends BaseDdlOperation{
    private SqlAlterFunction sqlAlterFunction;

    public LogicalAlterFunction(AlterFunction alterFunction) {
        super(alterFunction);
        this.sqlAlterFunction = (SqlAlterFunction) relDdl.sqlNode;
    }

    public static LogicalAlterFunction create(AlterFunction alterFunction) {
        return new LogicalAlterFunction(alterFunction);
    }

    public SqlAlterFunction getSqlAlterFunction() {
        return sqlAlterFunction;
    }
}
