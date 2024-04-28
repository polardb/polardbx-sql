package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableDiscardTableSpace extends SqlDdl {
    /**
     * Creates a SqlDdl.
     */
    private static final SqlOperator OPERATOR = new SqlAlterTableDiscardTableSpaceOperator();
    private SqlIdentifier tableName;
    private final String sourceSql;

    public SqlAlterTableDiscardTableSpace(SqlParserPos pos, SqlIdentifier tableName,
                                          String sourceSql) {
        super(OPERATOR, pos);
        this.tableName = tableName;
        this.sourceSql = sourceSql;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public String toString() {
        return sourceSql;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public SqlIdentifier getTableName() {
        return tableName;
    }

    @Override
    public SqlNode getTargetTable() {
        return tableName;
    }

    public static class SqlAlterTableDiscardTableSpaceOperator extends SqlSpecialOperator {

        public SqlAlterTableDiscardTableSpaceOperator() {
            super("ALTER_TABLE_DISCARD_TABLESPACE", SqlKind.ALTER_TABLE_DISCARD_TABLESPACE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("ALTER_TABLE_DISCARD_TABLESPACE_RESULT",
                    0,
                    columnType)));
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print(toString());
    }

}
