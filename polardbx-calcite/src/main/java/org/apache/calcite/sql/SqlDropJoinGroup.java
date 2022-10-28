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
public class SqlDropJoinGroup extends SqlDdl {

    private static final SqlSpecialOperator OPERATOR = new SqlDropTableGroupOperator();

    final boolean ifExists;

    final String joinGroupName;
    final String schemaName;

    public SqlDropJoinGroup(SqlParserPos pos, boolean ifExists, String schemaName, String joinGroupName) {
        super(OPERATOR, pos);
        this.ifExists = ifExists;
        this.schemaName = schemaName;
        this.joinGroupName = joinGroupName;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP TABLEGROUP");

        if (ifExists) {
            writer.keyword("IF EXISTS");
        }

        if (schemaName != null) {
            writer.keyword(schemaName + ".");
        }
        writer.keyword(joinGroupName);
    }

    public boolean isIfExists() {
        return ifExists;
    }

    public String getJoinGroupName() {
        return joinGroupName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    public static class SqlDropTableGroupOperator extends SqlSpecialOperator {

        public SqlDropTableGroupOperator() {
            super("DROP_JOINGROUP", SqlKind.DROP_JOINGROUP);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("DROP_JOINGROUP_RESULT",
                    0,
                    columnType)));
        }
    }
}

