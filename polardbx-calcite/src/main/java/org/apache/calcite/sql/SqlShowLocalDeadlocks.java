package org.apache.calcite.sql;

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
 * @author wuzhe
 */
public class SqlShowLocalDeadlocks extends SqlShow {

    private static final SqlSpecialOperator OPERATOR = new SqlShowLocalDeadlocksOperator();

    public SqlShowLocalDeadlocks(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers) {
        super(pos, specialIdentifiers);
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_LOCAL_DEADLOCKS;
    }

    public static class SqlShowLocalDeadlocksOperator extends SqlSpecialOperator {

        public SqlShowLocalDeadlocksOperator() {
            super("SHOW_LOCAL_DEADLOCKS", SqlKind.SHOW_LOCAL_DEADLOCKS);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("STORAGE_INST_ID", 0,
                typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("LOG", 1,
                typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            return typeFactory.createStructType(columns);
        }
    }
}
