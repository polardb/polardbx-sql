package org.apache.calcite.sql;

import java.util.LinkedList;
import java.util.List;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

public class SqlCheckColumnarPartition extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlCheckColumnarPartitionOperator();

    private SqlNode tableName;

    public SqlCheckColumnarPartition(SqlParserPos pos, SqlNode tableName) {
        super(pos);
        this.tableName = tableName;
    }

    public SqlNode getTableName() {
        return tableName;
    }

    public void setTableNames(SqlNode tableName) {
        this.tableName = tableName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("CHECK COLUMNAR PARTITION");
        tableName.unparse(writer, leftPrec, rightPrec);
        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CHECK_COLUMNAR_PARTITION;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
    }

    public static class SqlCheckColumnarPartitionOperator extends SqlSpecialOperator {

        public SqlCheckColumnarPartitionOperator() {
            super("CHECK_COLUMNAR_PARTITION", SqlKind.CHECK_COLUMNAR_PARTITION);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("Logical Table", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Columnar Index", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Partition", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(
                new RelDataTypeFieldImpl("Orc Files", 3, typeFactory.createSqlType(SqlTypeName.INTEGER_UNSIGNED)));
            columns.add(
                new RelDataTypeFieldImpl("Orc Rows", 4, typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
            columns.add(
                new RelDataTypeFieldImpl("Csv Files", 5, typeFactory.createSqlType(SqlTypeName.INTEGER_UNSIGNED)));
            columns.add(
                new RelDataTypeFieldImpl("Csv Rows", 6, typeFactory.createSqlType(SqlTypeName.BIGINT_UNSIGNED)));
            columns.add(new RelDataTypeFieldImpl("Extra", 7, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            return typeFactory.createStructType(columns);
        }
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlCheckColumnarPartition(this.pos, tableName);
    }
}
