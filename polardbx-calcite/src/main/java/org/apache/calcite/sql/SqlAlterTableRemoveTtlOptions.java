package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.ArrayList;
import java.util.List;

/**.
 *
 * @author chenghui.lch
 */
public class SqlAlterTableRemoveTtlOptions extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("REMOVE TTL", SqlKind.REMOVE_TTL);

    public SqlAlterTableRemoveTtlOptions(SqlParserPos pos) {
        super(pos);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        List<SqlNode> opList = new ArrayList<>();
        return opList;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT, "REMOVE TTL", "");
        writer.endList(frame);
    }
}