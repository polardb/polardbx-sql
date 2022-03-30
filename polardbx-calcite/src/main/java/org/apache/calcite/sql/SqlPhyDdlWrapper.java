package org.apache.calcite.sql;


import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.Arrays;
import java.util.List;

public class SqlPhyDdlWrapper extends SqlDdl{

    protected static final SqlOperator DDL_OPERATOR = new SqlSpecialOperator("DDL", SqlKind.OTHER_DDL);

    protected String phySql;

    /**
     * Creates a SqlDdl.
     */
    private SqlPhyDdlWrapper(SqlOperator sqlOperator, String phySql) {
        super(sqlOperator, SqlParserPos.ZERO);
        this.phySql = phySql;
    }

    public static SqlPhyDdlWrapper createForAllocateLocalPartition(SqlIdentifier tableName, String phySql){
        SqlPhyDdlWrapper sqlPhyDdlWrapper =
            new SqlPhyDdlWrapper(new SqlSpecialOperator("DDL", SqlKind.ALLOCATE_LOCAL_PARTITION), phySql);
        sqlPhyDdlWrapper.name = tableName;
        return sqlPhyDdlWrapper;
    }

    @Override
    public String toString() {
        return phySql;
    }

    @Override
    public SqlString toSqlString(SqlDialect dialect) {
        return new SqlString(dialect, phySql);
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name);
    }
}