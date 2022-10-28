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
 * @author guxu
 */
public class SqlContinueSchedule extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlContinueScheduleOperator();

    final long scheduleId;

    public SqlContinueSchedule(SqlParserPos pos, long scheduleId) {
        super(pos);
        this.scheduleId= scheduleId;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CONTINUE SCHEDULE");
        writer.keyword(String.valueOf(scheduleId));
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CONTINUE_SCHEDULE;
    }

    public long getScheduleId() {
        return this.scheduleId;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    public static class SqlContinueScheduleOperator extends SqlSpecialOperator {

        public SqlContinueScheduleOperator() {
            super("CONTINUE_SCHEDULE", SqlKind.CONTINUE_SCHEDULE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("CONTINUE_SCHEDULE",
                    0,
                    columnType)));
        }
    }
}

