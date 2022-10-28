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
public class SqlMergeTableGroup extends SqlDdl {
    /**
     * Creates a SqlDdl.
     */
    private static final SqlOperator OPERATOR = new SqlMergeTableGroupOperator();
    final String targetTableGroup;
    final List<String> sourceTableGroups;
    private final String sourceSql;
    private final boolean force;

    public SqlMergeTableGroup(SqlParserPos pos, String targetTableGroup, List<String> sourceTableGroups,
                              String sourceSql, boolean force) {
        super(OPERATOR, pos);
        this.targetTableGroup = targetTableGroup;
        this.sourceTableGroups = sourceTableGroups;
        this.sourceSql = sourceSql;
        this.force = force;
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

    public String getTargetTableGroup() {
        return targetTableGroup;
    }

    public List<String> getSourceTableGroups() {
        return sourceTableGroups;
    }

    public boolean isForce() {
        return force;
    }

    public static class SqlMergeTableGroupOperator extends SqlSpecialOperator {

        public SqlMergeTableGroupOperator() {
            super("MERGE_TABLEGROUP", SqlKind.MERGE_TABLEGROUP);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("MERGE_TABLEGROUP_RESULT",
                    0,
                    columnType)));
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print(toString());
    }

}
