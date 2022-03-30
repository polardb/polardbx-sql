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
import org.apache.commons.lang3.StringUtils;

/**
 * @author guxu
 */
public class SqlCreateSchedule extends SqlDal {
    private static final SqlSpecialOperator OPERATOR = new SqlCreateScheduleOperator();

    private String schemaName;

    private SqlNode tableName;

    private String cronExpr;
    private String timeZone;

    private boolean ifNotExists;

    public SqlCreateSchedule(SqlParserPos pos,
                             boolean ifNotExists,
                             String schemaName,
                             SqlNode tableName,
                             String cronExpr,
                             String timeZone) {
        super(pos);
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.cronExpr = cronExpr;
        this.timeZone = timeZone;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE SCHEDULE");

        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }

        writer.keyword("FOR LOCAL_PARTITION ON");
        tableName.unparse(writer, leftPrec, rightPrec);
        writer.keyword("CRON");
        writer.keyword(cronExpr);
        if(StringUtils.isNotEmpty(timeZone)){
            writer.keyword("TIMEZONE");
            writer.keyword(timeZone);
        }
    }


    @Override
    public SqlKind getKind() {
        return SqlKind.CREATE_SCHEDULE;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public static class SqlCreateScheduleOperator extends SqlSpecialOperator {
        public SqlCreateScheduleOperator() {
            super("CREATE_SCHEDULE", SqlKind.CREATE_SCHEDULE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory
                .createStructType(ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("CREATE_SCHEDULE_RESULT",
                    0,
                    columnType)));
        }
    }

    public String getSchemaName() {
        return this.schemaName;
    }

    public void setSchemaName(final String schemaName) {
        this.schemaName = schemaName;
    }

    @Override
    public SqlNode getTableName() {
        return this.tableName;
    }

    @Override
    public void setTableName(final SqlNode tableName) {
        this.tableName = tableName;
    }

    public String getCronExpr() {
        return this.cronExpr;
    }

    public void setCronExpr(final String cronExpr) {
        this.cronExpr = cronExpr;
    }

    public String getTimeZone() {
        return this.timeZone;
    }

    public void setTimeZone(final String timeZone) {
        this.timeZone = timeZone;
    }

    public boolean isIfNotExists() {
        return this.ifNotExists;
    }

    public void setIfNotExists(final boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }
}
