package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.List;

public class SqlCheckColumnarSnapshot extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlCheckColumnarSnapshotOperator();

    private SqlNode tableName;

    public SqlCheckColumnarSnapshot(SqlParserPos pos, SqlNode tableName) {
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
        writer.sep("CHECK COLUMNAR SNAPSHOT");
        tableName.unparse(writer, leftPrec, rightPrec);
        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CHECK_COLUMNAR_SNAPSHOT;
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
    }

    public static class SqlCheckColumnarSnapshotOperator extends SqlSpecialOperator {

        public SqlCheckColumnarSnapshotOperator() {
            super("CHECK_COLUMNAR_SNAPSHOT", SqlKind.CHECK_COLUMNAR_SNAPSHOT);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new ArrayList<>();
            columns.add(
                new RelDataTypeFieldImpl("Columnar_Index_Name", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Partition_Name", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Result", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Diff", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Advice", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            return typeFactory.createStructType(columns);
        }
    }

    @Override
    public SqlNode clone(SqlParserPos pos) {
        return new SqlCheckColumnarSnapshot(this.pos, tableName);
    }
}
