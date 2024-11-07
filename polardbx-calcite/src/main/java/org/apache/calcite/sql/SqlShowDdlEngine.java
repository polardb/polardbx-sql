package org.apache.calcite.sql;

import lombok.Getter;
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
 * @version 1.0
 */
public class SqlShowDdlEngine extends SqlShow {

    private SqlSpecialOperator operator;

    private boolean full;

    public SqlShowDdlEngine(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers,
                            boolean full) {
        super(pos, specialIdentifiers);
        this.full = full;
    }

    public boolean isFull() {
        return full;
    }

    public void setFull(boolean full) {
        this.full = full;
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        if (null == operator) {
            operator = new SqlShowDdlEngineOperator(this.full);
        }
        return operator;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_DDL_ENGINE;
    }

    public static class SqlShowDdlEngineOperator extends SqlSpecialOperator {

        private boolean full;

        public SqlShowDdlEngineOperator(boolean full) {
            super("SHOW_DDL_ENGINE", SqlKind.SHOW_DDL_ENGINE);
            this.full = full;
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();

            columns.add(new RelDataTypeFieldImpl("JOB_ID", 0, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            if (full) {
                columns.add(
                    new RelDataTypeFieldImpl("PARENT_JOB_ID", 1, typeFactory.createSqlType(SqlTypeName.INTEGER)));
            }
            columns.add(new RelDataTypeFieldImpl("SERVER", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("OBJECT_SCHEMA", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("OBJECT_NAME", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            if (full) {
                columns.add(
                    new RelDataTypeFieldImpl("NEW_OBJECT_NAME", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            }
            columns.add(new RelDataTypeFieldImpl("JOB_TYPE", 6, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("PHASE", 7, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("STATE", 8, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("PROGRESS", 9, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            if (full) {
                columns.add(new RelDataTypeFieldImpl("DDL_STMT", 10, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            }
            columns.add(new RelDataTypeFieldImpl("GMT_CREATED", 11, typeFactory.createSqlType(SqlTypeName.TIMESTAMP)));
            columns.add(new RelDataTypeFieldImpl("GMT_MODIFIED", 12, typeFactory.createSqlType(SqlTypeName.TIMESTAMP)));
            columns.add(new RelDataTypeFieldImpl("REMARK", 13, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(
                new RelDataTypeFieldImpl("BACKFILL_PROGRESS", 14, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }
}
