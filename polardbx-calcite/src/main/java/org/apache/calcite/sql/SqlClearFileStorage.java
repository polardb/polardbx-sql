package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.List;

public class SqlClearFileStorage extends SqlDdl {

    private static final SqlOperator OPERATOR = new SqlClearFilesStorageOperator();

    /**
     * Creates a SqlDdl.
     */
    public SqlClearFileStorage(SqlIdentifier name) {
        super(OPERATOR, SqlParserPos.ZERO);
        this.name = name;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.<SqlNode>of(name);
    }

    public SqlIdentifier getName() {
        return (SqlIdentifier) name;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CLEAR_FILESTORAGE;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(getOperator().getName());
        name.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateDdl(this, validator.getUnknownType(), scope);
    }

    public static class SqlClearFilesStorageOperator extends SqlSpecialOperator {
        public SqlClearFilesStorageOperator() {
            super("CLEAR FILESTORAGE", SqlKind.CLEAR_FILESTORAGE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new ArrayList<>();
            columns.add(new RelDataTypeFieldImpl(
                "Clear_File_Storage_Count",
                0,
                typeFactory.createSqlType(SqlTypeName.INTEGER_UNSIGNED)
            ));
            return typeFactory.createStructType(columns);
        }
    }
}
