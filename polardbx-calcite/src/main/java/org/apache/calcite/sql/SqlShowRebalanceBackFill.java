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
 * @author guxu.ygh
 */
public class SqlShowRebalanceBackFill extends SqlShow {

    private static final SqlSpecialOperator OPERATOR = new SqlShowRebalanceBackFillOperator();

    public SqlShowRebalanceBackFill(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                                    SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit){
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit);
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
        return SqlKind.SHOW_REBALANCE_BACKFILL;
    }

    public static class SqlShowRebalanceBackFillOperator extends SqlSpecialOperator {

        public SqlShowRebalanceBackFillOperator(){
            super("SHOW_REBALANCE_BACKFILL", SqlKind.SHOW_REBALANCE_BACKFILL);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            int index = 0;
            columns.add(new RelDataTypeFieldImpl("DDL_JOB_ID", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("CURRENT_SPEED(ROWS/SEC)", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("AVERAGE_SPEED(ROWS/SEC)", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("FINISHED_ROWS", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("APPROXIMATE_TOTAL_ROWS", index++, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }
}
