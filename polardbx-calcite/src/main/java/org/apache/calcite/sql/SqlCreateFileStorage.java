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
import org.apache.calcite.util.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlCreateFileStorage extends SqlDdl {
    private static final SqlOperator OPERATOR = new SqlCreateFileStorageOperator();

    private SqlIdentifier engineName;
    private List<Pair<SqlIdentifier, SqlIdentifier>> with;

    public SqlCreateFileStorage(SqlParserPos pos,
                                SqlIdentifier engineName,
                                List<Pair<SqlIdentifier, SqlIdentifier>> with) {
        super(OPERATOR, pos);
        this.engineName = engineName;
        this.with = with;
    }

    public SqlIdentifier getEngineName() {
        return engineName;
    }

    public SqlCreateFileStorage setEngineName(SqlIdentifier engineName) {
        this.engineName = engineName;
        return this;
    }

    public List<Pair<SqlIdentifier, SqlIdentifier>> getWith() {
        return with;
    }

    public Map<String, String> getWithValue() {
        Map<String, String> map = new HashMap<>();
        for (Pair<SqlIdentifier, SqlIdentifier> pair : with) {
            map.put(pair.left.toString().toUpperCase(), pair.right.toString());
        }
        return map;
    }

    public SqlCreateFileStorage setWith(
        List<Pair<SqlIdentifier, SqlIdentifier>> with) {
        this.with = with;
        return this;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "CREATE FILESTORAGE", "");

        engineName.unparse(writer, leftPrec, rightPrec);

        final SqlWriter.Frame withFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, " WITH (", ")");

        for (Pair<SqlIdentifier, SqlIdentifier> pair : with) {
            pair.left.unparse(writer, leftPrec, rightPrec);
            writer.sep("=");
            pair.right.unparse(writer, leftPrec, rightPrec);
        }

        writer.endList(withFrame);

        writer.endList(frame);
    }

    public static class SqlCreateFileStorageOperator extends SqlSpecialOperator {

        public SqlCreateFileStorageOperator(){
            super("CREATE FILESTORAGE", SqlKind.CREATE_FILESTORAGE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("CREATE FILESTORAGE RESULT",
                0, columnType)));
        }
    }
}
