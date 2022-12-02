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
public class SqlPauseSchedule extends SqlDal {

    private static final SqlSpecialOperator OPERATOR = new SqlPauseScheduleOperator();

    final boolean ifExists;
    final long scheduleId;
    final boolean forAllLocalPartition;

    public SqlPauseSchedule(SqlParserPos pos, boolean ifExists, long scheduleId) {
        super(pos);
        this.ifExists = ifExists;
        this.scheduleId= scheduleId;
        this.forAllLocalPartition = false;
    }

    public SqlPauseSchedule(SqlParserPos pos, boolean forAllLocalPartition) {
        super(pos);
        this.ifExists = false;
        this.scheduleId= 0L;
        this.forAllLocalPartition = forAllLocalPartition;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("PAUSE SCHEDULE");

        if (ifExists) {
            writer.keyword("IF EXISTS");
        }

        writer.keyword(String.valueOf(scheduleId));
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.PAUSE_SCHEDULE;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public boolean isForAllLocalPartition() {
        return forAllLocalPartition;
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

    public static class SqlPauseScheduleOperator extends SqlSpecialOperator {

        public SqlPauseScheduleOperator() {
            super("PAUSE_SCHEDULE", SqlKind.PAUSE_SCHEDULE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("PAUSE_SCHEDULE",
                    0,
                    columnType)));
        }
    }
}

