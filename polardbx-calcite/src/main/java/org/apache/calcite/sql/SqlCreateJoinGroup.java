package org.apache.calcite.sql;

import com.alibaba.polardbx.common.utils.TStringUtil;
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
public class SqlCreateJoinGroup extends SqlDdl {

    private static final SqlSpecialOperator OPERATOR = new SqlCreateTableGroupOperator();

    final boolean ifNotExists;
    final String locality;

    final String tableJoinName;
    final String schemaName;

    public SqlCreateJoinGroup(SqlParserPos pos, boolean ifNotExists, String schemaName,
                              String tableJoinName, String locality) {
        super(OPERATOR, pos);
        this.ifNotExists = ifNotExists;
        this.schemaName = schemaName;
        this.tableJoinName = tableJoinName;
        this.locality = locality;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE JOINGROUP");

        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }

        if (schemaName != null) {
            writer.keyword(schemaName + ".");
        }
        writer.keyword(tableJoinName);

        if (TStringUtil.isNotEmpty(locality)) {
            writer.keyword("LOCALITY = ");
            writer.print(TStringUtil.quoteString(locality));
        }
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public String getTableJoinName() {
        return tableJoinName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getLocality() {
        return locality;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    public static class SqlCreateTableGroupOperator extends SqlSpecialOperator {

        public SqlCreateTableGroupOperator() {
            super("CREATE_JOINGROUP", SqlKind.CREATE_JOINGROUP);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("CREATE_JOINGROUP_RESULT",
                    0,
                    columnType)));
        }
    }
}

