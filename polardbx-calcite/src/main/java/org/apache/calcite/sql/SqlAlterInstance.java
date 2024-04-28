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
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class SqlAlterInstance extends SqlDdl {
    private static final SqlSpecialOperator OPERATOR = new SqlAlterInstanceOperator();
    private List<SqlSetOption> optitionList;

    public SqlAlterInstance(SqlParserPos pos, List<SqlSetOption> optitionList) {
        super(OPERATOR, pos);
        this.optitionList = optitionList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER INSTANCE");

        for (SqlSetOption sqlSetOption : optitionList) {
            sqlSetOption.unparse(writer, leftPrec, rightPrec);
        }
    }

    public List<SqlSetOption> getOptitionList() {
        return optitionList;
    }

    public void setOptitionList(List<SqlSetOption> optitionList) {
        this.optitionList = optitionList;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    public static class SqlAlterInstanceOperator extends SqlSpecialOperator {
        public SqlAlterInstanceOperator() {
            super("ALTER_INSTANCE", SqlKind.ALTER_INSTANCE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("ALTER_INSTANCE_RESULT", 0,
                    columnType)));
        }
    }

}
