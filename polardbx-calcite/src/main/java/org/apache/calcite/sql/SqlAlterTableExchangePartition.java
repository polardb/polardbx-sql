package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

public class SqlAlterTableExchangePartition extends SqlAlterSpecification {
    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("EXCHANGE PARTITION", SqlKind.EXCHANGE_PARTITION);

    private final boolean validation;
    private final SqlNode tableName;
    private final List<SqlNode> partitions;

    public SqlAlterTableExchangePartition(SqlParserPos pos, boolean validation,
                                          SqlNode tableName, List<SqlNode> partitions) {
        super(pos);
        this.validation = validation;
        this.tableName = tableName;
        this.partitions = partitions;
    }

    public boolean isValidation() {
        return validation;
    }

    public SqlNode getTableName() {
        return tableName;
    }

    public List<SqlNode> getPartitions() {
        return partitions;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.<SqlNode>builder()
            .addAll(partitions)
            .add(tableName)
            .add(new SqlLiteral(validation, SqlTypeName.BOOLEAN, SqlParserPos.ZERO))
            .build();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "EXCHANGE", "");

        writer.keyword("PARTITION");
        int i = 0;
        for (SqlNode sqlNode : partitions) {
            sqlNode.unparse(writer, leftPrec, rightPrec);
            i++;
            if (i < partitions.size()) {
                writer.sep(",");
            }
        }

        writer.keyword("WITH TABLE");
        this.tableName.unparse(writer, leftPrec, rightPrec);

        if (validation) {
            writer.keyword("WITH VALIDATION");
        } else {
            writer.keyword("WITHOUT VALIDATION");
        }

        writer.endList(frame);
    }
}
