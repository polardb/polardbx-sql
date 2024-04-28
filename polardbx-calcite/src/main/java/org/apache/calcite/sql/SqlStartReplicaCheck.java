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
 * @since 2023/11/9 11:09
 **/
public class SqlStartReplicaCheck extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlStartReplicaCheckOperator();

    private SqlNode dbName;
    private SqlNode tableName;
    private SqlNode channel;

    public SqlStartReplicaCheck(SqlParserPos pos, SqlNode channel, SqlNode dbName) {
        super(pos);
        this.dbName = dbName;
        this.channel = channel;
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

    public SqlNode getChannel() {
        return channel;
    }

    public void setChannel(SqlNode channel) {
        this.channel = channel;
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
        if (channel != null) {
            writer.keyword("FOR CHANNEL");
            channel.unparse(writer, 0, 0);
        }
    }

    public static class SqlStartReplicaCheckOperator extends SqlSpecialOperator {

        public SqlStartReplicaCheckOperator() {
            super("START_REPLICA_CHECK", SqlKind.START_REPLICA_CHECK);
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
