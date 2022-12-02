package org.apache.calcite.sql;

import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlConvertAllSequences extends SqlDal {

    private static final SqlSpecialOperator OPERATOR =
        new SqlAffectedRowsOperator("CONVERT_ALL_SEQUENCES", SqlKind.CONVERT_ALL_SEQUENCES);

    private final Type fromType;
    private final Type toType;
    private final String schemaName;
    private final boolean allSchemata;

    public SqlConvertAllSequences(SqlParserPos pos, Type fromType, Type toType, String schemaName,
                                  boolean allSchemata) {
        super(pos);
        this.fromType = fromType;
        this.toType = toType;
        this.schemaName = schemaName;
        this.allSchemata = allSchemata;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.keyword("CONVERT ALL SEQUENCES FROM");
        writer.print(fromType.name());
        writer.keyword("TO");
        writer.print(toType.name());
        if (!allSchemata) {
            writer.keyword("FOR");
            writer.print(schemaName);
        }
        writer.endList(selectFrame);
    }

    public Type getFromType() {
        return fromType;
    }

    public Type getToType() {
        return toType;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public boolean isAllSchemata() {
        return allSchemata;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CONVERT_ALL_SEQUENCES;
    }

}
