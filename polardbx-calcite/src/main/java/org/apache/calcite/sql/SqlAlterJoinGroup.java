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
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterJoinGroup extends SqlDdl {
    /**
     * Creates a SqlDdl.
     */
    private static final SqlOperator OPERATOR = new SqlAlterJoinGroupOperator();
    final String joinGroupName;
    private final List<String> tableNames;
    private final boolean isAdd;
    private final String sourceSql;

    public SqlAlterJoinGroup(SqlParserPos pos, String joinGroupName, List<String> tableNames,
                             boolean isAdd,
                             String sourceSql) {
        super(OPERATOR, pos);
        this.joinGroupName = joinGroupName;
        this.tableNames = tableNames;
        this.isAdd = isAdd;
        this.sourceSql = sourceSql;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public String toString() {
        return sourceSql;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public String getJoinGroupName() {
        return joinGroupName;
    }

    public boolean isAdd() {
        return isAdd;
    }

    public static class SqlAlterJoinGroupOperator extends SqlSpecialOperator {

        public SqlAlterJoinGroupOperator() {
            super("ALTER_JOINGROUP", SqlKind.ALTER_JOINGROUP);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("ALTER_JOINGROUP_RESULT",
                    0,
                    columnType)));
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print(toString());
    }

}
