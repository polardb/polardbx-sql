package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.LinkedList;
import java.util.List;

/**
 * @author yudong
 * @since 2023/11/9 11:11
 **/
public class SqlShowReplicaCheckProgress extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlShowReplicaCheckProgressOperator();

    private SqlNode dbName;
    private SqlNode tableName;

    public SqlShowReplicaCheckProgress(SqlParserPos pos, SqlNode dbName) {
        super(pos);
        this.dbName = dbName;
    }

    public SqlShowReplicaCheckProgress(SqlParserPos pos, SqlNode dbName, SqlNode tableName) {
        super(pos);
        this.dbName = dbName;
        this.tableName = tableName;
    }

    public SqlNode getDbName() {
        return dbName;
    }

    public void setDbName(SqlNode dbName) {
        this.dbName = dbName;
    }

    public SqlNode getTableName() {
        return tableName;
    }

    public void setTableName(SqlNode tableName) {
        this.tableName = tableName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CHECK REPLICA TABLE");
        dbName.unparse(writer, 0, 0);
        if (tableName != null) {
            writer.print(".");
            tableName.unparse(writer, 0, 0);
        }
        writer.keyword("SHOW PROGRESS");
    }

    public static class SqlShowReplicaCheckProgressOperator extends SqlSpecialOperator {

        public SqlShowReplicaCheckProgressOperator() {
            super("SHOW_REPLICA_CHECK_PROGRESS", SqlKind.SHOW_REPLICA_CHECK_PROGRESS);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("RESULT", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            return typeFactory.createStructType(columns);
        }
    }
}
